package main

import (
	"context"
	"log"
	"net"

	pb "ecommerce-microservices/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type customerServer struct {
	pb.UnimplementedCustomerServiceServer
}

var customers = map[string]*pb.Customer{
	"C001": {
		CustomerId: "C001",
		Name:       "Lhundup Dorji",
		Email:      "lhundup@example.com",
	},
	"C002": {
		CustomerId: "C002",
		Name:       "Karma Wangchuk",
		Email:      "karma@example.com",
	},
	"C003": {
		CustomerId: "C003",
		Name:       "Sonam Dorji",
		Email:      "sonam@example.com",
	},
}

func (s *customerServer) GetCustomer(
	ctx context.Context,
	req *pb.GetCustomerRequest,
) (*pb.Customer, error) {

	customer, exists := customers[req.CustomerId]

	if !exists {
		return nil, status.Errorf(
			codes.NotFound,
			"customer %s not found",
			req.CustomerId,
		)
	}

	return customer, nil
}

func main() {

	listener, err := net.Listen("tcp", ":50052")

	if err != nil {
		log.Fatalf("Failed to listen on port 50052: %v", err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterCustomerServiceServer(
		grpcServer,
		&customerServer{},
	)

	reflection.Register(grpcServer)

	log.Println("Customer Service running on port 50052")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}