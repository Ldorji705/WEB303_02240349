package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"ecommerce-microservices/pkg/resilience"
	pb "ecommerce-microservices/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ---- gRPC clients for each downstream service ----
var (
	productClient   pb.ProductServiceClient
	customerClient  pb.CustomerServiceClient
	inventoryClient pb.InventoryServiceClient
	paymentClient   pb.PaymentServiceClient
)

// ---- One circuit breaker PER downstream service ----
// Kept separate so a failure in one service does not falsely trip
// the breaker for a completely unrelated service.
var (
	productBreaker   = resilience.NewCircuitBreaker(3, 5*time.Second)
	customerBreaker  = resilience.NewCircuitBreaker(3, 5*time.Second)
	inventoryBreaker = resilience.NewCircuitBreaker(3, 5*time.Second)
	paymentBreaker   = resilience.NewCircuitBreaker(3, 5*time.Second)
)

// apiError is the standard JSON error shape returned to the UI.
type apiError struct {
	Error string `json:"error"`
}

// writeGrpcError maps a gRPC/circuit-breaker error to an HTTP status + JSON body.
func writeGrpcError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case errors.Is(err, resilience.ErrCircuitOpen):
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(apiError{Error: "service is currently unavailable (circuit open) — please try again shortly"})
	case status.Code(err) == codes.AlreadyExists:
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(apiError{Error: status.Convert(err).Message()})
	case status.Code(err) == codes.NotFound:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(apiError{Error: "resource not found"})
	case status.Code(err) == codes.InvalidArgument:
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(apiError{Error: status.Convert(err).Message()})
	case status.Code(err) == codes.DeadlineExceeded:
		w.WriteHeader(http.StatusGatewayTimeout)
		json.NewEncoder(w).Encode(apiError{Error: "downstream service took too long to respond"})
	default:
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(apiError{Error: "downstream service is unavailable"})
	}
}

// resilientCall wraps any downstream gRPC call with CircuitBreaker(Retry(Timeout(call))).
func resilientCall(breaker *resilience.CircuitBreaker, fn func(ctx context.Context) error) error {
	return breaker.Execute(func() error {
		return resilience.CallWithRetry(context.Background(), 3, 300*time.Millisecond, func(ctx context.Context) error {
			return resilience.CallWithTimeout(ctx, 3*time.Second, fn)
		})
	})
}

// ---- Handlers ----

// fetchProduct performs a resilient call to product-service and returns the result.
// Extracted from the handler so it can be tested directly (see product_integration_test.go).
func fetchProduct(client pb.ProductServiceClient, breaker *resilience.CircuitBreaker, id string) (*pb.Product, error) {
	var product *pb.Product
	err := resilientCall(breaker, func(ctx context.Context) error {
		resp, callErr := client.GetProduct(ctx, &pb.GetProductRequest{ProductId: id})
		if callErr != nil {
			return callErr
		}
		product = resp
		return nil
	})
	return product, err
}

func getProductHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	w.Header().Set("Content-Type", "application/json")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(apiError{Error: "product id is required"})
		return
	}

	product, err := fetchProduct(productClient, productBreaker, id)
	if err != nil {
		writeGrpcError(w, err)
		return
	}
	json.NewEncoder(w).Encode(product)
}

// fetchCustomer performs a resilient call to customer-service and returns the result.
// Extracted from the handler so it can be tested directly (see customer_integration_test.go).
func fetchCustomer(client pb.CustomerServiceClient, breaker *resilience.CircuitBreaker, id string) (*pb.Customer, error) {
	var customer *pb.Customer
	err := resilientCall(breaker, func(ctx context.Context) error {
		resp, callErr := client.GetCustomer(ctx, &pb.GetCustomerRequest{CustomerId: id})
		if callErr != nil {
			return callErr
		}
		customer = resp
		return nil
	})
	return customer, err
}

func getCustomerHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	w.Header().Set("Content-Type", "application/json")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(apiError{Error: "customer id is required"})
		return
	}

	customer, err := fetchCustomer(customerClient, customerBreaker, id)
	if err != nil {
		writeGrpcError(w, err)
		return
	}
	json.NewEncoder(w).Encode(customer)
}

// fetchStock performs a resilient call to inventory-service and returns the result.
// Extracted from the handler so it can be tested directly (see inventory_integration_test.go).
func fetchStock(client pb.InventoryServiceClient, breaker *resilience.CircuitBreaker, id string) (*pb.StockResponse, error) {
	var stock *pb.StockResponse
	err := resilientCall(breaker, func(ctx context.Context) error {
		resp, callErr := client.CheckStock(ctx, &pb.CheckStockRequest{ProductId: id})
		if callErr != nil {
			return callErr
		}
		stock = resp
		return nil
	})
	return stock, err
}

func getInventoryHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	w.Header().Set("Content-Type", "application/json")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(apiError{Error: "product id is required"})
		return
	}

	stock, err := fetchStock(inventoryClient, inventoryBreaker, id)
	if err != nil {
		writeGrpcError(w, err)
		return
	}
	json.NewEncoder(w).Encode(stock)
}

// fetchPayment performs a resilient call to payment-service and returns the result.
// Extracted from the handler so it can be tested directly (see payment_integration_test.go).
func fetchPayment(client pb.PaymentServiceClient, breaker *resilience.CircuitBreaker, orderID string, amount float64) (*pb.PaymentResponse, error) {
	var payment *pb.PaymentResponse
	err := resilientCall(breaker, func(ctx context.Context) error {
		resp, callErr := client.ProcessPayment(ctx, &pb.PaymentRequest{
			OrderId: orderID,
			Amount:  amount,
		})
		if callErr != nil {
			return callErr
		}
		payment = resp
		return nil
	})
	return payment, err
}

func postPaymentHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var body struct {
		OrderID string  `json:"orderId"`
		Amount  float64 `json:"amount"`
	}
	if r.Body == nil || json.NewDecoder(r.Body).Decode(&body) != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(apiError{Error: "invalid request body"})
		return
	}

	payment, err := fetchPayment(paymentClient, paymentBreaker, body.OrderID, body.Amount)
	if err != nil {
		writeGrpcError(w, err)
		return
	}
	json.NewEncoder(w).Encode(payment)
}

// statusHandler exposes the live circuit breaker state for each downstream
// service, so the UI can show real-time system health.
func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"product":   productBreaker.State().String(),
		"customer":  customerBreaker.State().String(),
		"inventory": inventoryBreaker.State().String(),
		"payment":   paymentBreaker.State().String(),
	})
}

func mustDial(target string) *grpc.ClientConn {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not connect to %s: %v", target, err)
	}
	return conn
}

func main() {
	productConn := dialGatewayService("PRODUCT_SERVICE_ADDR", "localhost:50051")
	defer productConn.Close()
	productClient = pb.NewProductServiceClient(productConn)

	customerConn := dialGatewayService("CUSTOMER_SERVICE_ADDR", "localhost:50052")
	defer customerConn.Close()
	customerClient = pb.NewCustomerServiceClient(customerConn)

	inventoryConn := dialGatewayService("INVENTORY_SERVICE_ADDR", "localhost:50053")
	defer inventoryConn.Close()
	inventoryClient = pb.NewInventoryServiceClient(inventoryConn)

	paymentConn := dialGatewayService("PAYMENT_SERVICE_ADDR", "localhost:50054")
	defer paymentConn.Close()
	paymentClient = pb.NewPaymentServiceClient(paymentConn)

	orderConn := dialGatewayService("ORDER_SERVICE_ADDR", "localhost:50052")
	defer orderConn.Close()
	orderClient = pb.NewOrderServiceClient(orderConn)

	mux := http.NewServeMux()

	mux.HandleFunc("/api/products", productsRESTHandler)
	mux.HandleFunc("/api/products/", productsRESTHandler)
	mux.HandleFunc("/api/orders", ordersRESTHandler)
	mux.HandleFunc("/api/orders/", ordersRESTHandler)

	mux.HandleFunc("/api/customers", getCustomerHandler)
	mux.HandleFunc("/api/inventory", getInventoryHandler)
	mux.HandleFunc("/api/payments", postPaymentHandler)
	mux.HandleFunc("/api/status", statusHandler)

	mux.Handle("/", http.FileServer(http.Dir("./api-gateway/static")))

	port := gatewayEnv("GATEWAY_PORT", "8080")
	log.Printf("API Gateway running on http://localhost:%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
