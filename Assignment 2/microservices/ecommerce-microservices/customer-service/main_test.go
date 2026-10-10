package main

import (
	"context"
	"testing"

	pb "ecommerce-microservices/proto"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetCustomer_ExistingRecord_ReturnsCorrectData(t *testing.T) {
	server := &customerServer{}
	req := &pb.GetCustomerRequest{CustomerId: "C001"}

	resp, err := server.GetCustomer(context.Background(), req)

	assert.NoError(t, err)
	assert.Equal(t, "C001", resp.CustomerId)
	assert.Equal(t, "Lhundup Dorji", resp.Name)
	assert.Equal(t, "lhundup@example.com", resp.Email)
}

func TestGetCustomer_DifferentIdentifiers_ReturnCorrectData(t *testing.T) {
	server := &customerServer{}

	testCases := []struct {
		id           string
		expectedName string
	}{
		{"C002", "Karma Wangchuk"},
		{"C003", "Sonam Dorji"},
	}

	for _, tc := range testCases {
		resp, err := server.GetCustomer(context.Background(), &pb.GetCustomerRequest{CustomerId: tc.id})
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedName, resp.Name)
	}
}

func TestGetCustomer_NonExistentRecord_ReturnsNotFound(t *testing.T) {
	server := &customerServer{}
	req := &pb.GetCustomerRequest{CustomerId: "C999"}

	resp, err := server.GetCustomer(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestGetCustomer_EmptyIdentifier_ReturnsNotFound(t *testing.T) {
	server := &customerServer{}
	req := &pb.GetCustomerRequest{CustomerId: ""}

	resp, err := server.GetCustomer(context.Background(), req)

	// customer-service currently has no explicit empty-ID validation,
	// so an empty ID falls through to the "not found" path — this test
	// documents and locks in that actual behaviour.
	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}