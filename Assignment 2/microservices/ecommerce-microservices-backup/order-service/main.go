package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"ecommerce-microservices/pkg/resilience"
	pb "ecommerce-microservices/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var breaker = resilience.NewCircuitBreaker(3, 5*time.Second)

func callDiscountFunction(total float64, discountPercent float64) error {
	requestData := map[string]float64{
		"total":           total,
		"discountPercent": discountPercent,
	}

	requestBody, err := json.Marshal(requestData)
	if err != nil {
		return fmt.Errorf("failed to create discount request: %w", err)
	}

	req, err := http.NewRequest(
		http.MethodPost,
		"http://localhost:3001/discount",
		bytes.NewBuffer(requestBody),
	)
	if err != nil {
		return fmt.Errorf("failed to create discount request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 3 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("discount function unavailable: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read discount response: %w", err)
	}

	var result struct {
		Success         bool    `json:"success"`
		OriginalTotal   float64 `json:"originalTotal"`
		DiscountPercent float64 `json:"discountPercent"`
		Discount        float64 `json:"discount"`
		FinalTotal      float64 `json:"finalTotal"`
		Error           string  `json:"error"`
	}

	if err := json.Unmarshal(responseBody, &result); err != nil {
		return fmt.Errorf("invalid discount function response: %w", err)
	}

	if !result.Success {
		return fmt.Errorf("discount function error: %s", result.Error)
	}

	fmt.Println()
	fmt.Println("Discount Calculation")
	fmt.Println("--------------------")
	fmt.Printf("Original Total: %.2f\n", result.OriginalTotal)
	fmt.Printf("Discount: %.2f%%\n", result.DiscountPercent)
	fmt.Printf("Discount Amount: %.2f\n", result.Discount)
	fmt.Printf("Final Total: %.2f\n", result.FinalTotal)

	return nil
}

func makeResilientCall(
	client pb.ProductServiceClient,
	productID string,
	requestNum int,
) {
	fmt.Printf("\n--- Request #%d (product %s) ---\n", requestNum, productID)

	var product *pb.Product
	start := time.Now()

	err := breaker.Execute(func() error {
		return resilience.CallWithRetry(
			context.Background(),
			3,
			300*time.Millisecond,
			func(ctx context.Context) error {
				return resilience.CallWithTimeout(
					ctx,
					3*time.Second,
					func(callCtx context.Context) error {
						resp, callErr := client.GetProduct(
							callCtx,
							&pb.GetProductRequest{
								ProductId: productID,
							},
						)

						if callErr != nil {
							return callErr
						}

						product = resp
						return nil
					},
				)
			},
		)
	})

	elapsed := time.Since(start)

	if err != nil {
		if errors.Is(err, resilience.ErrCircuitOpen) {
			log.Printf(
				"REJECTED instantly by circuit breaker after %v (state: %s)\n",
				elapsed,
				breaker.State(),
			)
		} else {
			log.Printf(
				"FAILED after %v: %v (breaker state: %s)\n",
				elapsed,
				err,
				breaker.State(),
			)
		}

		return
	}

	fmt.Println("Product Information")
	fmt.Println("--------------------")
	fmt.Printf("Product ID: %s\n", product.ProductId)
	fmt.Printf("Name: %s\n", product.Name)
	fmt.Printf("Price: %.2f\n", product.Price)

	discountErr := callDiscountFunction(product.Price, 10)

	if discountErr != nil {
		log.Printf("Discount calculation failed: %v\n", discountErr)
		return
	}

	log.Printf(
		"Succeeded in %v (breaker state: %s)\n",
		elapsed,
		breaker.State(),
	)
}

func main() {
	if len(os.Args) < 2 {
		log.Println(
			"Usage: go run ./order-service <product_id> [num_calls] [interval_seconds]",
		)
		log.Println(
			"  num_calls        - how many sequential requests to make (default 1)",
		)
		log.Println(
			"  interval_seconds - delay between requests (default 1)",
		)
		return
	}

	productID := os.Args[1]

	numCalls := 1

	if len(os.Args) >= 3 {
		if n, err := strconv.Atoi(os.Args[2]); err == nil {
			numCalls = n
		}
	}

	interval := 1 * time.Second

	if len(os.Args) >= 4 {
		if s, err := strconv.Atoi(os.Args[3]); err == nil {
			interval = time.Duration(s) * time.Second
		}
	}

	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		log.Fatalf(
			"Could not connect to Product Service: %v",
			err,
		)
	}

	defer conn.Close()

	client := pb.NewProductServiceClient(conn)

	for i := 1; i <= numCalls; i++ {
		makeResilientCall(
			client,
			productID,
			i,
		)

		if i < numCalls {
			time.Sleep(interval)
		}
	}
}
