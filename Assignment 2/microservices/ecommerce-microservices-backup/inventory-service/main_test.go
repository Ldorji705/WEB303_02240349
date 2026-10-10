package main

import (
	"context"
	"testing"

	pb "ecommerce-microservices/proto"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCheckStock_ExistingRecord_ReturnsCorrectData(t *testing.T) {
	server := &inventoryServer{}
	req := &pb.CheckStockRequest{ProductId: "P001"}

	resp, err := server.CheckStock(context.Background(), req)

	assert.NoError(t, err)
	assert.Equal(t, "P001", resp.ProductId)
	assert.Equal(t, int32(10), resp.Quantity)
}

func TestCheckStock_DifferentIdentifiers_ReturnCorrectData(t *testing.T) {
	server := &inventoryServer{}

	testCases := []struct {
		id               string
		expectedQuantity int32
	}{
		{"P002", 25},
		{"P003", 50},
		{"P004", 15},
	}

	for _, tc := range testCases {
		resp, err := server.CheckStock(context.Background(), &pb.CheckStockRequest{ProductId: tc.id})
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedQuantity, resp.Quantity)
	}
}

func TestCheckStock_NonExistentRecord_ReturnsNotFound(t *testing.T) {
	server := &inventoryServer{}
	req := &pb.CheckStockRequest{ProductId: "P999"}

	resp, err := server.CheckStock(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestCheckStock_EmptyIdentifier_ReturnsNotFound(t *testing.T) {
	server := &inventoryServer{}
	req := &pb.CheckStockRequest{ProductId: ""}

	resp, err := server.CheckStock(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestReduceStock_ValidQuantity_ReducesCorrectly(t *testing.T) {
	server := &inventoryServer{}
	// Use P002 (starts at 25) so this test doesn't clash with other tests
	// running concurrently against the same in-memory map.
	req := &pb.ReduceStockRequest{ProductId: "P002", Quantity: 5}

	resp, err := server.ReduceStock(context.Background(), req)

	assert.NoError(t, err)
	assert.Equal(t, int32(20), resp.Quantity)
}

func TestReduceStock_InvalidQuantity_ReturnsInvalidArgument(t *testing.T) {
	server := &inventoryServer{}
	req := &pb.ReduceStockRequest{ProductId: "P003", Quantity: 0}

	resp, err := server.ReduceStock(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestReduceStock_InsufficientStock_ReturnsFailedPrecondition(t *testing.T) {
	server := &inventoryServer{}
	req := &pb.ReduceStockRequest{ProductId: "P004", Quantity: 9999}

	resp, err := server.ReduceStock(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}