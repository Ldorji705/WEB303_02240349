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

// mockCustomerServer is a stand-in for the real customer-service, whose
// behaviour each test controls directly (success, error, or delay).
type mockCustomerServer struct {
	pb.UnimplementedCustomerServiceServer
	delay     time.Duration
	err       error
	customer  *pb.Customer
	callCount int32
}

func (m *mockCustomerServer) GetCustomer(ctx context.Context, req *pb.GetCustomerRequest) (*pb.Customer, error) {
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
	return m.customer, nil
}

// startMockCustomerService spins up an in-memory gRPC server backed by the
// mock, and returns a real gRPC client wired to it via bufconn. This lets
// us test the ACTUAL client code (fetchCustomer + resilience patterns)
// against a controllable fake server, with no real network or process needed.
func startMockCustomerService(t *testing.T, mock *mockCustomerServer) (pb.CustomerServiceClient, func()) {
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterCustomerServiceServer(grpcServer, mock)

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

	return pb.NewCustomerServiceClient(conn), cleanup
}

// ---------- Scenario: dependency available ----------

func TestIntegration_CustomerService_Available_Succeeds(t *testing.T) {
	mock := &mockCustomerServer{customer: &pb.Customer{CustomerId: "C001", Name: "Tashi Wangmo"}}
	client, cleanup := startMockCustomerService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	customer, err := fetchCustomer(client, breaker, "C001")

	assert.NoError(t, err)
	assert.Equal(t, "Tashi Wangmo", customer.Name)
}

func TestIntegration_CustomerService_DifferentIdentifiers_Succeed(t *testing.T) {
	mock := &mockCustomerServer{customer: &pb.Customer{CustomerId: "C002", Name: "Pema Choden"}}
	client, cleanup := startMockCustomerService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	customer, err := fetchCustomer(client, breaker, "C002")

	assert.NoError(t, err)
	assert.Equal(t, "Pema Choden", customer.Name)
}

func TestIntegration_CustomerService_InvalidIdentifier_ReturnsError(t *testing.T) {
	mock := &mockCustomerServer{err: status.Error(codes.InvalidArgument, "customer id must not be empty")}
	client, cleanup := startMockCustomerService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	customer, err := fetchCustomer(client, breaker, "")

	assert.Error(t, err)
	assert.Nil(t, customer)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// ---------- Scenario: dependency unavailable (service is down) ----------

func TestIntegration_CustomerService_Unavailable_ReturnsError(t *testing.T) {
	lis := bufconn.Listen(bufSize)
	lis.Close() // nothing is listening — simulates the service being down

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, _ := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer conn.Close()
	client := pb.NewCustomerServiceClient(conn)

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second)
	customer, err := fetchCustomer(client, breaker, "C001")

	assert.Error(t, err)
	assert.Nil(t, customer)
}

// ---------- Scenario: dependency returns an error (NotFound) ----------

func TestIntegration_CustomerService_NotFound_DoesNotRetry(t *testing.T) {
	mock := &mockCustomerServer{err: status.Error(codes.NotFound, "customer not found")}
	client, cleanup := startMockCustomerService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	customer, err := fetchCustomer(client, breaker, "C999")

	assert.Error(t, err)
	assert.Nil(t, customer)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, int32(1), atomic.LoadInt32(&mock.callCount)) // proves NO retries happened
}

func TestIntegration_CustomerService_TransientError_RetriesThenFails(t *testing.T) {
	mock := &mockCustomerServer{err: status.Error(codes.Unavailable, "simulated failure")}
	client, cleanup := startMockCustomerService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second) // high threshold so the breaker doesn't trip mid-test
	customer, err := fetchCustomer(client, breaker, "C001")

	assert.Error(t, err)
	assert.Nil(t, customer)
	assert.Equal(t, int32(3), atomic.LoadInt32(&mock.callCount)) // proves 3 retry attempts happened
}

// ---------- Scenario: dependency slower than the configured timeout ----------

func TestIntegration_CustomerService_SlowerThanTimeout_TimesOut(t *testing.T) {
	mock := &mockCustomerServer{
		delay:    5 * time.Second, // slower than the 3s timeout used in resilientCall
		customer: &pb.Customer{CustomerId: "C001", Name: "Tashi Wangmo"},
	}
	client, cleanup := startMockCustomerService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second)

	start := time.Now()
	customer, err := fetchCustomer(client, breaker, "C001")
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Nil(t, customer)
	// 3 attempts × 3s timeout + retry backoffs (300ms + 600ms) ≈ 9.9s total
	assert.GreaterOrEqual(t, elapsed, 9*time.Second)
	assert.Less(t, elapsed, 11*time.Second)
}