package main

import (
	"context"
	"testing"

	pb "ecommerce-microservices/proto"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetProduct_ExistingRecord_ReturnsCorrectData(t *testing.T) {
	server := &productServer{}
	req := &pb.GetProductRequest{ProductId: "P001"}

	resp, err := server.GetProduct(context.Background(), req)

	assert.NoError(t, err)
	assert.Equal(t, "P001", resp.ProductId)
	assert.Equal(t, "Laptop", resp.Name)
	assert.Equal(t, float64(75000), resp.Price)
}

func TestGetProduct_DifferentIdentifiers_ReturnCorrectData(t *testing.T) {
	server := &productServer{}

	testCases := []struct {
		id            string
		expectedName  string
		expectedPrice float64
	}{
		{"P002", "Mechanical Keyboard", 4500},
		{"P003", "Wireless Mouse", 1800},
		{"P004", "Monitor", 25000},
	}

	for _, tc := range testCases {
		resp, err := server.GetProduct(context.Background(), &pb.GetProductRequest{ProductId: tc.id})
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedName, resp.Name)
		assert.Equal(t, tc.expectedPrice, resp.Price)
	}
}

func TestGetProduct_NonExistentRecord_ReturnsNotFound(t *testing.T) {
	server := &productServer{}
	req := &pb.GetProductRequest{ProductId: "P999"}

	resp, err := server.GetProduct(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestGetProduct_EmptyIdentifier_ReturnsInvalidArgument(t *testing.T) {
	server := &productServer{}
	req := &pb.GetProductRequest{ProductId: ""}

	resp, err := server.GetProduct(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

