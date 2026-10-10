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

// mockPaymentServer is a stand-in for the real payment-service, whose
// behaviour each test controls directly (success, error, or delay).
type mockPaymentServer struct {
	pb.UnimplementedPaymentServiceServer
	delay     time.Duration
	err       error
	payment   *pb.PaymentResponse
	callCount int32
}

func (m *mockPaymentServer) ProcessPayment(ctx context.Context, req *pb.PaymentRequest) (*pb.PaymentResponse, error) {
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
	return m.payment, nil
}

// startMockPaymentService spins up an in-memory gRPC server backed by the
// mock, and returns a real gRPC client wired to it via bufconn. This lets
// us test the ACTUAL client code (fetchPayment + resilience patterns)
// against a controllable fake server, with no real network or process needed.
func startMockPaymentService(t *testing.T, mock *mockPaymentServer) (pb.PaymentServiceClient, func()) {
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterPaymentServiceServer(grpcServer, mock)

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

	return pb.NewPaymentServiceClient(conn), cleanup
}

// ---------- Scenario: dependency available ----------

func TestIntegration_PaymentService_Available_Succeeds(t *testing.T) {
	mock := &mockPaymentServer{payment: &pb.PaymentResponse{PaymentId: "PAY001", OrderId: "O001", Amount: 1000, Status: "SUCCESS", Message: "payment processed"}}
	client, cleanup := startMockPaymentService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	payment, err := fetchPayment(client, breaker, "O001", 1000)

	assert.NoError(t, err)
	assert.Equal(t, "SUCCESS", payment.Status)
}

func TestIntegration_PaymentService_DifferentIdentifiers_Succeed(t *testing.T) {
	mock := &mockPaymentServer{payment: &pb.PaymentResponse{PaymentId: "PAY002", OrderId: "O002", Amount: 2500, Status: "SUCCESS", Message: "payment processed"}}
	client, cleanup := startMockPaymentService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	payment, err := fetchPayment(client, breaker, "O002", 2500)

	assert.NoError(t, err)
	assert.Equal(t, "O002", payment.OrderId)
}

func TestIntegration_PaymentService_InvalidRequest_ReturnsError(t *testing.T) {
	mock := &mockPaymentServer{err: status.Error(codes.InvalidArgument, "order id must not be empty")}
	client, cleanup := startMockPaymentService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	payment, err := fetchPayment(client, breaker, "", 0)

	assert.Error(t, err)
	assert.Nil(t, payment)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// ---------- Scenario: dependency unavailable (service is down) ----------

func TestIntegration_PaymentService_Unavailable_ReturnsError(t *testing.T) {
	lis := bufconn.Listen(bufSize)
	lis.Close() // nothing is listening — simulates the service being down

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, _ := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer conn.Close()
	client := pb.NewPaymentServiceClient(conn)

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second)
	payment, err := fetchPayment(client, breaker, "O001", 1000)

	assert.Error(t, err)
	assert.Nil(t, payment)
}

// ---------- Scenario: dependency returns an error (NotFound) ----------

func TestIntegration_PaymentService_NotFound_DoesNotRetry(t *testing.T) {
	mock := &mockPaymentServer{err: status.Error(codes.NotFound, "order not found")}
	client, cleanup := startMockPaymentService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(3, 5*time.Second)
	payment, err := fetchPayment(client, breaker, "O999", 1000)

	assert.Error(t, err)
	assert.Nil(t, payment)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, int32(1), atomic.LoadInt32(&mock.callCount)) // proves NO retries happened
}

func TestIntegration_PaymentService_TransientError_RetriesThenFails(t *testing.T) {
	mock := &mockPaymentServer{err: status.Error(codes.Unavailable, "simulated failure")}
	client, cleanup := startMockPaymentService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second) // high threshold so the breaker doesn't trip mid-test
	payment, err := fetchPayment(client, breaker, "O001", 1000)

	assert.Error(t, err)
	assert.Nil(t, payment)
	assert.Equal(t, int32(3), atomic.LoadInt32(&mock.callCount)) // proves 3 retry attempts happened
}

// ---------- Scenario: dependency slower than the configured timeout ----------

func TestIntegration_PaymentService_SlowerThanTimeout_TimesOut(t *testing.T) {
	mock := &mockPaymentServer{
		delay:   5 * time.Second, // slower than the 3s timeout used in resilientCall
		payment: &pb.PaymentResponse{PaymentId: "PAY001", OrderId: "O001", Amount: 1000, Status: "SUCCESS", Message: "payment processed"},
	}
	client, cleanup := startMockPaymentService(t, mock)
	defer cleanup()

	breaker := resilience.NewCircuitBreaker(5, 5*time.Second)

	start := time.Now()
	payment, err := fetchPayment(client, breaker, "O001", 1000)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Nil(t, payment)
	// 3 attempts × 3s timeout + retry backoffs (300ms + 600ms) ≈ 9.9s total
	assert.GreaterOrEqual(t, elapsed, 9*time.Second)
	assert.Less(t, elapsed, 11*time.Second)
}