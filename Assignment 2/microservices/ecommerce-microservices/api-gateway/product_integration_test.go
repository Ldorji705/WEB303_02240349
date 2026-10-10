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

const bufSize = 1024 * 1024

// mockProductServer is a stand-in for the real product-service, whose
// behaviour each test controls directly (success, error, or delay).
type mockProductServer struct {
	pb.UnimplementedProductServiceServer
	delay     time.Duration
	err       error
	product   *pb.Product
	callCount int32
}

func (m *mockProductServer) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.Product, error) {
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
	return m.product, nil
}

// startMockProductService spins up an in-memory gRPC server backed by the
// mock, and returns a real gRPC client wired to it via bufconn. This lets
// us test the ACTUAL client code (fetchProduct + resilience patterns)
// against a controllable fake server, with no real network or process needed.
func startMockProductService(t *testing.T, mock *mockProductServer) (pb.ProductServiceClient, func()) {
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterProductServiceServer(grpcServer, mock)

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

	return pb.NewProductServiceClient(conn), cleanup
}

// ---------- Scenario: dependency available ----------

func TestIntegration_ProductService_Available_Succeeds(t *testing.T) {
	mock := &mockProductServer{product: &pb.Product{ProductId: "P001", Name: "Laptop", Price: 75000}}
	client, cleanup := startMockProductService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	product, err := fetchProduct(client, breaker, "P001")

	assert.NoError(t, err)
	assert.Equal(t, "Laptop", product.Name)
}

func TestIntegration_ProductService_DifferentIdentifiers_Succeed(t *testing.T) {
	mock := &mockProductServer{product: &pb.Product{ProductId: "P002", Name: "Keyboard", Price: 4500}}
	client, cleanup := startMockProductService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	product, err := fetchProduct(client, breaker, "P002")

	assert.NoError(t, err)
	assert.Equal(t, "Keyboard", product.Name)
}

func TestIntegration_ProductService_InvalidIdentifier_ReturnsError(t *testing.T) {
	mock := &mockProductServer{err: status.Error(codes.InvalidArgument, "product id must not be empty")}
	client, cleanup := startMockProductService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	product, err := fetchProduct(client, breaker, "")

	assert.Error(t, err)
	assert.Nil(t, product)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// ---------- Scenario: dependency unavailable (service is down) ----------

func TestIntegration_ProductService_Unavailable_ReturnsError(t *testing.T) {
	lis := bufconn.Listen(bufSize)
	lis.Close() // nothing is listening — simulates the service being down

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, _ := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer conn.Close()
	client := pb.NewProductServiceClient(conn)

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second)
	product, err := fetchProduct(client, breaker, "P001")

	assert.Error(t, err)
	assert.Nil(t, product)
}

// ---------- Scenario: dependency returns an error (NotFound) ----------

func TestIntegration_ProductService_NotFound_DoesNotRetry(t *testing.T) {
	mock := &mockProductServer{err: status.Error(codes.NotFound, "product not found")}
	client, cleanup := startMockProductService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	product, err := fetchProduct(client, breaker, "P999")

	assert.Error(t, err)
	assert.Nil(t, product)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, int32(1), atomic.LoadInt32(&mock.callCount)) // proves NO retries happened
}

func TestIntegration_ProductService_TransientError_RetriesThenFails(t *testing.T) {
	mock := &mockProductServer{err: status.Error(codes.Unavailable, "simulated failure")}
	client, cleanup := startMockProductService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second) // high threshold so the breaker doesn't trip mid-test
	product, err := fetchProduct(client, breaker, "P001")

	assert.Error(t, err)
	assert.Nil(t, product)
	assert.Equal(t, int32(3), atomic.LoadInt32(&mock.callCount)) // proves 3 retry attempts happened
}

// ---------- Scenario: dependency slower than the configured timeout ----------

func TestIntegration_ProductService_SlowerThanTimeout_TimesOut(t *testing.T) {
	mock := &mockProductServer{
		delay:   5 * time.Second, // slower than the 3s timeout used in resilientCall
		product: &pb.Product{ProductId: "P001", Name: "Laptop", Price: 75000},
	}
	client, cleanup := startMockProductService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second)

	start := time.Now()
	product, err := fetchProduct(client, breaker, "P001")
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Nil(t, product)
	// 3 attempts × 3s timeout + retry backoffs (300ms + 600ms) ≈ 9.9s total —
	// proves the call fails via timeout/retry exhaustion rather than hanging
	// indefinitely or succeeding despite the 5s mock delay.
	assert.GreaterOrEqual(t, elapsed, 9*time.Second)
	assert.Less(t, elapsed, 11*time.Second)
}