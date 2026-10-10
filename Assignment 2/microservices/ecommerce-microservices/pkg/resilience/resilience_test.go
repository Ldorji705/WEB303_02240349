package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------- Timeout tests ----------

func TestCallWithTimeout_CompletesInTime_Succeeds(t *testing.T) {
	err := CallWithTimeout(context.Background(), 100*time.Millisecond, func(ctx context.Context) error {
		return nil
	})
	assert.NoError(t, err)
}

func TestCallWithTimeout_ExceedsDeadline_ReturnsError(t *testing.T) {
	err := CallWithTimeout(context.Background(), 50*time.Millisecond, func(ctx context.Context) error {
		select {
		case <-time.After(200 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

// ---------- Retry tests ----------

func TestCallWithRetry_SucceedsOnFirstTry(t *testing.T) {
	attempts := 0
	err := CallWithRetry(context.Background(), 3, 10*time.Millisecond, func(ctx context.Context) error {
		attempts++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, attempts)
}

func TestCallWithRetry_SucceedsAfterTransientFailures(t *testing.T) {
	attempts := 0
	err := CallWithRetry(context.Background(), 3, 10*time.Millisecond, func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return status.Error(codes.Unavailable, "temporary failure")
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestCallWithRetry_NonTransientError_DoesNotRetry(t *testing.T) {
	attempts := 0
	err := CallWithRetry(context.Background(), 3, 10*time.Millisecond, func(ctx context.Context) error {
		attempts++
		return status.Error(codes.NotFound, "does not exist")
	})
	assert.Error(t, err)
	assert.Equal(t, 1, attempts)
}

func TestCallWithRetry_ExhaustsAllAttempts_ReturnsError(t *testing.T) {
	attempts := 0
	err := CallWithRetry(context.Background(), 3, 5*time.Millisecond, func(ctx context.Context) error {
		attempts++
		return status.Error(codes.Unavailable, "always fails")
	})
	assert.Error(t, err)
	assert.Equal(t, 3, attempts)
}

// ---------- Circuit Breaker tests ----------

func TestCircuitBreaker_StartsClosed(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)
	assert.Equal(t, StateClosed, cb.State())
}

func TestCircuitBreaker_OpensAfterThresholdFailures(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)
	failingFn := func() error { return errors.New("boom") }

	cb.Execute(failingFn)
	cb.Execute(failingFn)
	assert.Equal(t, StateClosed, cb.State())

	cb.Execute(failingFn)
	assert.Equal(t, StateOpen, cb.State())
}

func TestCircuitBreaker_RejectsRequestsWhenOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 1*time.Second)
	cb.Execute(func() error { return errors.New("boom") })

	callCount := 0
	err := cb.Execute(func() error {
		callCount++
		return nil
	})

	assert.ErrorIs(t, err, ErrCircuitOpen)
	assert.Equal(t, 0, callCount)
}

func TestCircuitBreaker_HalfOpenAfterResetTimeout(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)
	cb.Execute(func() error { return errors.New("boom") })
	assert.Equal(t, StateOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	err := cb.Execute(func() error { return nil })
	assert.NoError(t, err)
	assert.Equal(t, StateClosed, cb.State())
}

func TestCircuitBreaker_ReopensIfHalfOpenTrialFails(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)
	cb.Execute(func() error { return errors.New("boom") })

	time.Sleep(60 * time.Millisecond)

	err := cb.Execute(func() error { return errors.New("still broken") })
	assert.Error(t, err)
	assert.Equal(t, StateOpen, cb.State())
}

