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

type inventoryServer struct {
	pb.UnimplementedInventoryServiceServer
}

var stock = map[string]int32{
	"P001": 10,
	"P002": 25,
	"P003": 50,
	"P004": 15,
}

func (s *inventoryServer) CheckStock(
	ctx context.Context,
	req *pb.CheckStockRequest,
) (*pb.StockResponse, error) {

	quantity, exists := stock[req.ProductId]

	if !exists {
		return nil, status.Errorf(
			codes.NotFound,
			"product %s not found in inventory",
			req.ProductId,
		)
	}

	return &pb.StockResponse{
		ProductId: req.ProductId,
		Quantity:  quantity,
		Message:   "Stock available",
	}, nil
}

func (s *inventoryServer) ReduceStock(
	ctx context.Context,
	req *pb.ReduceStockRequest,
) (*pb.StockResponse, error) {

	quantity, exists := stock[req.ProductId]

	if !exists {
		return nil, status.Errorf(
			codes.NotFound,
			"product %s not found in inventory",
			req.ProductId,
		)
	}

	if req.Quantity <= 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"quantity must be greater than zero",
		)
	}

	if quantity < req.Quantity {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"insufficient stock for product %s",
			req.ProductId,
		)
	}

	stock[req.ProductId] -= req.Quantity

	return &pb.StockResponse{
		ProductId: req.ProductId,
		Quantity:  stock[req.ProductId],
		Message:   "Stock reduced successfully",
	}, nil
}

func main() {

	listener, err := net.Listen("tcp", ":50053")

	if err != nil {
		log.Fatalf("Failed to listen on port 50053: %v", err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterInventoryServiceServer(
		grpcServer,
		&inventoryServer{},
	)

	reflection.Register(grpcServer)

	log.Println("Inventory Service running on port 50053")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}