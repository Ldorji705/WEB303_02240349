package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"ecommerce-microservices/pkg/resilience"
	pb "ecommerce-microservices/proto"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// mockInventoryServer is a stand-in for the real inventory-service, whose
// behaviour each test controls directly (success, error, or delay).
type mockInventoryServer struct {
	pb.UnimplementedInventoryServiceServer
	delay     time.Duration
	err       error
	stock     *pb.StockResponse
	callCount int32
}

func (m *mockInventoryServer) CheckStock(ctx context.Context, req *pb.CheckStockRequest) (*pb.StockResponse, error) {
	atomic.AddInt32(&m.callCount, 1)
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.stock, nil
}

// startMockInventoryService spins up an in-memory gRPC server backed by the
// mock, and returns a real gRPC client wired to it via bufconn. This lets
// us test the ACTUAL client code (fetchStock + resilience patterns)
// against a controllable fake server, with no real network or process needed.
func startMockInventoryService(t *testing.T, mock *mockInventoryServer) (pb.InventoryServiceClient, func()) {
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterInventoryServiceServer(grpcServer, mock)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
	}

	return pb.NewInventoryServiceClient(conn), cleanup
}

// ---------- Scenario: dependency available ----------

func TestIntegration_InventoryService_Available_Succeeds(t *testing.T) {
	mock := &mockInventoryServer{stock: &pb.StockResponse{ProductId: "P001", Quantity: 42, Message: "in stock"}}
	client, cleanup := startMockInventoryService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	stock, err := fetchStock(client, breaker, "P001")

	assert.NoError(t, err)
	assert.Equal(t, int32(42), stock.Quantity)
}

func TestIntegration_InventoryService_DifferentIdentifiers_Succeed(t *testing.T) {
	mock := &mockInventoryServer{stock: &pb.StockResponse{ProductId: "P002", Quantity: 7, Message: "low stock"}}
	client, cleanup := startMockInventoryService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	stock, err := fetchStock(client, breaker, "P002")

	assert.NoError(t, err)
	assert.Equal(t, int32(7), stock.Quantity)
}

func TestIntegration_InventoryService_InvalidIdentifier_ReturnsError(t *testing.T) {
	mock := &mockInventoryServer{err: status.Error(codes.InvalidArgument, "product id must not be empty")}
	client, cleanup := startMockInventoryService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	stock, err := fetchStock(client, breaker, "")

	assert.Error(t, err)
	assert.Nil(t, stock)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// ---------- Scenario: dependency unavailable (service is down) ----------

func TestIntegration_InventoryService_Unavailable_ReturnsError(t *testing.T) {
	lis := bufconn.Listen(bufSize)
	lis.Close() // nothing is listening — simulates the service being down

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, _ := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer conn.Close()
	client := pb.NewInventoryServiceClient(conn)

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second)
	stock, err := fetchStock(client, breaker, "P001")

	assert.Error(t, err)
	assert.Nil(t, stock)
}

// ---------- Scenario: dependency returns an error (NotFound) ----------

func TestIntegration_InventoryService_NotFound_DoesNotRetry(t *testing.T) {
	mock := &mockInventoryServer{err: status.Error(codes.NotFound, "product not found")}
	client, cleanup := startMockInventoryService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	stock, err := fetchStock(client, breaker, "P999")

	assert.Error(t, err)
	assert.Nil(t, stock)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, int32(1), atomic.LoadInt32(&mock.callCount)) // proves NO retries happened
}

func TestIntegration_InventoryService_TransientError_RetriesThenFails(t *testing.T) {
	mock := &mockInventoryServer{err: status.Error(codes.Unavailable, "simulated failure")}
	client, cleanup := startMockInventoryService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second) // high threshold so the breaker doesn't trip mid-test
	stock, err := fetchStock(client, breaker, "P001")

	assert.Error(t, err)
	assert.Nil(t, stock)
	assert.Equal(t, int32(3), atomic.LoadInt32(&mock.callCount)) // proves 3 retry attempts happened
}

// ---------- Scenario: dependency slower than the configured timeout ----------

func TestIntegration_InventoryService_SlowerThanTimeout_TimesOut(t *testing.T) {
	mock := &mockInventoryServer{
		delay: 5 * time.Second, // slower than the 3s timeout used in resilientCall
		stock: &pb.StockResponse{ProductId: "P001", Quantity: 42, Message: "in stock"},
	}
	client, cleanup := startMockInventoryService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second)

	start := time.Now()
	stock, err := fetchStock(client, breaker, "P001")
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Nil(t, stock)
	// 3 attempts × 3s timeout + retry backoffs (300ms + 600ms) ≈ 9.9s total
	assert.GreaterOrEqual(t, elapsed, 9*time.Second)
	assert.Less(t, elapsed, 11*time.Second)
}