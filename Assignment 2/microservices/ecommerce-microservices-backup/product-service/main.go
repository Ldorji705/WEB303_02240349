package main

import (
	"context"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	pb "ecommerce-microservices/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// products is the in-memory catalog used by this demo service.
var products = map[string]*pb.Product{
	"P001": {ProductId: "P001", Name: "Laptop", Price: 75000},
	"P002": {ProductId: "P002", Name: "Mechanical Keyboard", Price: 4500},
	"P003": {ProductId: "P003", Name: "Wireless Mouse", Price: 1800},
	"P004": {ProductId: "P004", Name: "Monitor", Price: 25000},
}

type productServer struct {
	pb.UnimplementedProductServiceServer
}

func (s *productServer) GetProduct(
	ctx context.Context,
	req *pb.GetProductRequest,
) (*pb.Product, error) {

	// --- Input validation ---
	if req.ProductId == "" {
		return nil, status.Error(codes.InvalidArgument, "product id must not be empty")
	}
	// --- End input validation ---

	// --- Failure simulation (for Timeout / Retry / Circuit Breaker demos) ---
	if delayMs, err := strconv.Atoi(os.Getenv("SIMULATE_DELAY_MS")); err == nil && delayMs > 0 {
		log.Printf("Simulating %dms delay before responding...", delayMs)
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}

	if failRate, err := strconv.ParseFloat(os.Getenv("SIMULATE_FAIL_RATE"), 64); err == nil && failRate > 0 {
		if rand.Float64() < failRate {
			log.Printf("Simulating failure for product %s", req.ProductId)
			return nil, status.Error(codes.Unavailable, "simulated service failure")
		}
	}
	// --- End failure simulation ---

	product, exists := products[req.ProductId]

	if !exists {
		return nil, status.Errorf(
			codes.NotFound,
			"product %s not found",
			req.ProductId,
		)
	}

	return product, nil
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterProductServiceServer(grpcServer, &productServer{})

	log.Printf("product-service listening on :%s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}