package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strings"
	"time"

	"ecommerce-microservices/pkg/resilience"
	pb "ecommerce-microservices/proto"

	"github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type orderServer struct {
	pb.UnimplementedOrderServiceServer
	db             *sql.DB
	productClient  pb.ProductServiceClient
	productBreaker *resilience.CircuitBreaker
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func connectDatabase() (*sql.DB, error) {
	host := envOrDefault("DB_HOST", "localhost")
	port := envOrDefault("DB_PORT", "5432")
	user := envOrDefault("DB_USER", "postgres")
	password := envOrDefault("DB_PASSWORD", "postgres")
	name := envOrDefault("DB_NAME", "orders")

	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, name,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Allow PostgreSQL time to become ready when the service starts.
	var pingErr error
	for attempt := 1; attempt <= 30; attempt++ {
		pingErr = db.Ping()
		if pingErr == nil {
			break
		}
		log.Printf("Waiting for Order database (attempt %d/30): %v", attempt, pingErr)
		time.Sleep(time.Second)
	}
	if pingErr != nil {
		db.Close()
		return nil, fmt.Errorf("could not connect to Order database: %w", pingErr)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			order_id VARCHAR(50) PRIMARY KEY,
			product_id VARCHAR(50) NOT NULL,
			product_name VARCHAR(200) NOT NULL,
			quantity INTEGER NOT NULL CHECK (quantity > 0),
			unit_price DOUBLE PRECISION NOT NULL CHECK (unit_price > 0),
			total_price DOUBLE PRECISION NOT NULL CHECK (total_price > 0),
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("could not create orders table: %w", err)
	}

	return db, nil
}

func validateOrder(orderID, productID string, quantity int32) error {
	if strings.TrimSpace(orderID) == "" || len(orderID) > 50 {
		return status.Error(codes.InvalidArgument, "order_id is required and must be at most 50 characters")
	}
	if strings.TrimSpace(productID) == "" || len(productID) > 50 {
		return status.Error(codes.InvalidArgument, "product_id is required and must be at most 50 characters")
	}
	if quantity <= 0 {
		return status.Error(codes.InvalidArgument, "quantity must be greater than zero")
	}
	return nil
}

func (s *orderServer) fetchProduct(ctx context.Context, productID string) (*pb.Product, error) {
	var product *pb.Product

	err := s.productBreaker.Execute(func() error {
		return resilience.CallWithRetry(ctx, 3, 300*time.Millisecond, func(retryCtx context.Context) error {
			return resilience.CallWithTimeout(retryCtx, 3*time.Second, func(callCtx context.Context) error {
				result, err := s.productClient.GetProduct(callCtx, &pb.GetProductRequest{
					ProductId: productID,
				})
				if err != nil {
					return err
				}
				if result == nil || result.ProductId == "" || result.Price <= 0 ||
					math.IsNaN(result.Price) || math.IsInf(result.Price, 0) {
					return status.Error(codes.Internal, "Product Service returned invalid product data")
				}
				product = result
				return nil
			})
		})
	})

	if err != nil {
		if errors.Is(err, resilience.ErrCircuitOpen) {
			return nil, status.Error(codes.Unavailable, "Product Service circuit breaker is open")
		}
		return nil, err
	}
	return product, nil
}

func mapDatabaseError(err error, entity string) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return status.Errorf(codes.NotFound, "%s not found", entity)
	}

	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23505" {
		return status.Errorf(codes.AlreadyExists, "%s already exists", entity)
	}

	return status.Errorf(codes.Internal, "database operation failed: %v", err)
}

func (s *orderServer) CreateOrder(ctx context.Context, req *pb.CreateOrderRequest) (*pb.Order, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateOrder(req.OrderId, req.ProductId, req.Quantity); err != nil {
		return nil, err
	}

	product, err := s.fetchProduct(ctx, req.ProductId)
	if err != nil {
		return nil, err
	}

	total := product.Price * float64(req.Quantity)
	if math.IsInf(total, 0) || math.IsNaN(total) {
		return nil, status.Error(codes.InvalidArgument, "order total is invalid")
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO orders (order_id, product_id, product_name, quantity, unit_price, total_price)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, req.OrderId, product.ProductId, product.Name, req.Quantity, product.Price, total)
	if err != nil {
		return nil, mapDatabaseError(err, "order")
	}

	return &pb.Order{
		OrderId: req.OrderId, ProductId: product.ProductId,
		ProductName: product.Name, Quantity: req.Quantity,
		UnitPrice: product.Price, TotalPrice: total,
	}, nil
}

func (s *orderServer) GetOrder(ctx context.Context, req *pb.GetOrderRequest) (*pb.Order, error) {
	if req == nil || strings.TrimSpace(req.OrderId) == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}

	order := &pb.Order{}
	err := s.db.QueryRowContext(ctx, `
		SELECT order_id, product_id, product_name, quantity, unit_price, total_price
		FROM orders WHERE order_id = $1
	`, req.OrderId).Scan(
		&order.OrderId, &order.ProductId, &order.ProductName,
		&order.Quantity, &order.UnitPrice, &order.TotalPrice,
	)
	if err != nil {
		return nil, mapDatabaseError(err, "order")
	}
	return order, nil
}

func (s *orderServer) ListOrders(ctx context.Context, _ *pb.ListOrdersRequest) (*pb.ListOrdersResponse, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT order_id, product_id, product_name, quantity, unit_price, total_price
		FROM orders ORDER BY created_at DESC, order_id
	`)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not list orders: %v", err)
	}
	defer rows.Close()

	response := &pb.ListOrdersResponse{}
	for rows.Next() {
		order := &pb.Order{}
		if err := rows.Scan(
			&order.OrderId, &order.ProductId, &order.ProductName,
			&order.Quantity, &order.UnitPrice, &order.TotalPrice,
		); err != nil {
			return nil, status.Errorf(codes.Internal, "could not read order: %v", err)
		}
		response.Orders = append(response.Orders, order)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "could not finish listing orders: %v", err)
	}
	return response, nil
}

func (s *orderServer) UpdateOrder(ctx context.Context, req *pb.UpdateOrderRequest) (*pb.Order, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateOrder(req.OrderId, req.ProductId, req.Quantity); err != nil {
		return nil, err
	}

	product, err := s.fetchProduct(ctx, req.ProductId)
	if err != nil {
		return nil, err
	}

	total := product.Price * float64(req.Quantity)
	if math.IsInf(total, 0) || math.IsNaN(total) {
		return nil, status.Error(codes.InvalidArgument, "order total is invalid")
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE orders
		SET product_id = $2, product_name = $3, quantity = $4,
		    unit_price = $5, total_price = $6
		WHERE order_id = $1
	`, req.OrderId, product.ProductId, product.Name, req.Quantity, product.Price, total)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not update order: %v", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not confirm order update: %v", err)
	}
	if count == 0 {
		return nil, status.Error(codes.NotFound, "order not found")
	}

	return &pb.Order{
		OrderId: req.OrderId, ProductId: product.ProductId,
		ProductName: product.Name, Quantity: req.Quantity,
		UnitPrice: product.Price, TotalPrice: total,
	}, nil
}

func (s *orderServer) DeleteOrder(ctx context.Context, req *pb.DeleteOrderRequest) (*pb.DeleteOrderResponse, error) {
	if req == nil || strings.TrimSpace(req.OrderId) == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}

	result, err := s.db.ExecContext(ctx, `DELETE FROM orders WHERE order_id = $1`, req.OrderId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not delete order: %v", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not confirm order deletion: %v", err)
	}
	if count == 0 {
		return nil, status.Error(codes.NotFound, "order not found")
	}

	return &pb.DeleteOrderResponse{Message: "Order deleted successfully"}, nil
}

func main() {
	db, err := connectDatabase()
	if err != nil {
		log.Fatalf("Order Service database connection failed: %v", err)
	}
	defer db.Close()

	productAddress := envOrDefault("PRODUCT_SERVICE_ADDR", "localhost:50051")
	productConn, err := grpc.NewClient(
		productAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Could not create Product Service connection: %v", err)
	}
	defer productConn.Close()

	port := envOrDefault("PORT", "50052")
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Could not listen on port %s: %v", port, err)
	}

	server := grpc.NewServer()
	pb.RegisterOrderServiceServer(server, &orderServer{
		db:             db,
		productClient:  pb.NewProductServiceClient(productConn),
		productBreaker: resilience.NewCircuitBreaker(3, 5*time.Second),
	})

	log.Printf("Order Service listening on :%s", port)
	log.Printf("Product Service address: %s", productAddress)

	if err := server.Serve(listener); err != nil {
		log.Fatalf("Order Service failed: %v", err)
	}
}
