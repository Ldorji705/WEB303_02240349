package main

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "ecommerce-microservices/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type paymentServer struct {
	pb.UnimplementedPaymentServiceServer
}

func (s *paymentServer) ProcessPayment(
	ctx context.Context,
	req *pb.PaymentRequest,
) (*pb.PaymentResponse, error) {

	if req.OrderId == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"order ID is required",
		)
	}

	if req.Amount <= 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"payment amount must be greater than zero",
		)
	}

	paymentID := fmt.Sprintf("PAY-%s", req.OrderId)

	return &pb.PaymentResponse{
		PaymentId: paymentID,
		OrderId:   req.OrderId,
		Amount:    req.Amount,
		Status:    "SUCCESS",
		Message:   "Payment processed successfully",
	}, nil
}

func main() {

	listener, err := net.Listen("tcp", ":50054")

	if err != nil {
		log.Fatalf("Failed to listen on port 50054: %v", err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterPaymentServiceServer(
		grpcServer,
		&paymentServer{},
	)

	reflection.Register(grpcServer)

	log.Println("Payment Service running on port 50054")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}