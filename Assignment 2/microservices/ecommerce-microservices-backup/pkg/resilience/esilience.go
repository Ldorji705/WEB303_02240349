package resilience

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------- Timeout ----------

func CallWithTimeout(parent context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return fn(ctx)
}

// ---------- Retry ----------

func IsTransient(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded
}

func CallWithRetry(ctx context.Context, maxAttempts int, baseBackoff time.Duration, fn func(ctx context.Context) error) error {
	backoff := baseBackoff
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}
		if !IsTransient(lastErr) {
			return lastErr
		}
		if attempt < maxAttempts {
			log.Printf("[Retry] attempt %d/%d failed (%v) — retrying in %v", attempt, maxAttempts, lastErr, backoff)
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return fmt.Errorf("all %d attempts failed: %w", maxAttempts, lastErr)
}

// ---------- Circuit Breaker ----------

type CBState int

const (
	StateClosed CBState = iota
	StateOpen
	StateHalfOpen
)

func (s CBState) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF-OPEN"
	default:
		return "UNKNOWN"
	}
}

var ErrCircuitOpen = errors.New("circuit breaker is open: request rejected without calling downstream")

type CircuitBreaker struct {
	mu               sync.Mutex
	state            CBState
	failureThreshold int
	resetTimeout     time.Duration
	consecutiveFails int
	lastFailureTime  time.Time
}

func NewCircuitBreaker(failureThreshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{state: StateClosed, failureThreshold: failureThreshold, resetTimeout: resetTimeout}
}

func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	if cb.state == StateOpen {
		if time.Since(cb.lastFailureTime) > cb.resetTimeout {
			cb.transition(StateHalfOpen)
		} else {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.consecutiveFails++
		cb.lastFailureTime = time.Now()
		if cb.state == StateHalfOpen {
			cb.transition(StateOpen)
		} else if cb.consecutiveFails >= cb.failureThreshold {
			cb.transition(StateOpen)
		}
		return err
	}

	if cb.state == StateHalfOpen {
		cb.transition(StateClosed)
	}
	cb.consecutiveFails = 0
	return nil
}

func (cb *CircuitBreaker) transition(newState CBState) {
	if cb.state != newState {
		log.Printf("[CircuitBreaker] %s -> %s", cb.state, newState)
	}
	cb.state = newState
	if newState == StateClosed {
		cb.consecutiveFails = 0
	}
}

func (cb *CircuitBreaker) State() CBState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}