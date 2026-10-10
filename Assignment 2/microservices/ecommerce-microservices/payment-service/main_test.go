package main

import (
	"context"
	"testing"

	pb "ecommerce-microservices/proto"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProcessPayment_ValidRequest_Succeeds(t *testing.T) {
	server := &paymentServer{}
	req := &pb.PaymentRequest{OrderId: "ORD-001", Amount: 500}

	resp, err := server.ProcessPayment(context.Background(), req)

	assert.NoError(t, err)
	assert.Equal(t, "PAY-ORD-001", resp.PaymentId)
	assert.Equal(t, "ORD-001", resp.OrderId)
	assert.Equal(t, float64(500), resp.Amount)
	assert.Equal(t, "SUCCESS", resp.Status)
}

func TestProcessPayment_DifferentOrders_ReturnCorrectData(t *testing.T) {
	server := &paymentServer{}

	testCases := []struct {
		orderID string
		amount  float64
	}{
		{"ORD-002", 1200},
		{"ORD-003", 75000},
	}

	for _, tc := range testCases {
		resp, err := server.ProcessPayment(context.Background(), &pb.PaymentRequest{OrderId: tc.orderID, Amount: tc.amount})
		assert.NoError(t, err)
		assert.Equal(t, tc.orderID, resp.OrderId)
		assert.Equal(t, tc.amount, resp.Amount)
	}
}

func TestProcessPayment_EmptyOrderId_ReturnsInvalidArgument(t *testing.T) {
	server := &paymentServer{}
	req := &pb.PaymentRequest{OrderId: "", Amount: 500}

	resp, err := server.ProcessPayment(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestProcessPayment_ZeroAmount_ReturnsInvalidArgument(t *testing.T) {
	server := &paymentServer{}
	req := &pb.PaymentRequest{OrderId: "ORD-004", Amount: 0}

	resp, err := server.ProcessPayment(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestProcessPayment_NegativeAmount_ReturnsInvalidArgument(t *testing.T) {
	server := &paymentServer{}
	req := &pb.PaymentRequest{OrderId: "ORD-005", Amount: -50}

	resp, err := server.ProcessPayment(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}