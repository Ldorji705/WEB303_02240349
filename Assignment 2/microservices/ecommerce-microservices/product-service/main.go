package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "ecommerce-microservices/proto"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var products = map[string]*pb.Product{
	"P001": {ProductId: "P001", Name: "Laptop", Price: 75000},
	"P002": {ProductId: "P002", Name: "Mechanical Keyboard", Price: 4500},
	"P003": {ProductId: "P003", Name: "Wireless Mouse", Price: 1800},
	"P004": {ProductId: "P004", Name: "Monitor", Price: 25000},
}

var productsMu sync.RWMutex

type productServer struct {
	pb.UnimplementedProductServiceServer
	db *sql.DB
}

func validateProduct(id, name string, price float64) error {
	if strings.TrimSpace(id) == "" {
		return status.Error(codes.InvalidArgument, "product id must not be empty")
	}
	if len(id) > 50 {
		return status.Error(codes.InvalidArgument, "product id must be at most 50 characters")
	}
	if strings.TrimSpace(name) == "" {
		return status.Error(codes.InvalidArgument, "product name must not be empty")
	}
	if len(name) > 200 {
		return status.Error(codes.InvalidArgument, "product name must be at most 200 characters")
	}
	if math.IsNaN(price) || math.IsInf(price, 0) || price <= 0 {
		return status.Error(codes.InvalidArgument, "product price must be a finite positive number")
	}
	return nil
}

func (s *productServer) simulateFailure(ctx context.Context, id string) error {
	if delayMs, err := strconv.Atoi(os.Getenv("SIMULATE_DELAY_MS")); err == nil && delayMs > 0 {
		log.Printf("Simulating %dms delay for product %s", delayMs, id)
		timer := time.NewTimer(time.Duration(delayMs) * time.Millisecond)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return status.Error(codes.DeadlineExceeded, "product request timed out")
		case <-timer.C:
		}
	}

	if failRate, err := strconv.ParseFloat(os.Getenv("SIMULATE_FAIL_RATE"), 64); err == nil && failRate > 0 {
		if rand.Float64() < math.Min(failRate, 1) {
			return status.Error(codes.Unavailable, "simulated product service failure")
		}
	}
	return nil
}

func (s *productServer) CreateProduct(ctx context.Context, req *pb.CreateProductRequest) (*pb.Product, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateProduct(req.ProductId, req.Name, req.Price); err != nil {
		return nil, err
	}

	p := &pb.Product{
		ProductId: strings.TrimSpace(req.ProductId),
		Name:      strings.TrimSpace(req.Name),
		Price:     req.Price,
	}

	if s.db != nil {
		_, err := s.db.ExecContext(ctx,
			`INSERT INTO products (product_id, name, price) VALUES ($1, $2, $3)`,
			p.ProductId, p.Name, p.Price,
		)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "duplicate key") {
				return nil, status.Error(codes.AlreadyExists, "product already exists")
			}
			return nil, status.Error(codes.Internal, "could not create product")
		}
		return p, nil
	}

	productsMu.Lock()
	defer productsMu.Unlock()
	if _, exists := products[p.ProductId]; exists {
		return nil, status.Error(codes.AlreadyExists, "product already exists")
	}
	products[p.ProductId] = p
	return p, nil
}

func (s *productServer) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.Product, error) {
	if req == nil || strings.TrimSpace(req.ProductId) == "" {
		return nil, status.Error(codes.InvalidArgument, "product id must not be empty")
	}
	if err := s.simulateFailure(ctx, req.ProductId); err != nil {
		return nil, err
	}

	if s.db != nil {
		p := &pb.Product{}
		err := s.db.QueryRowContext(ctx,
			`SELECT product_id, name, price FROM products WHERE product_id = $1`,
			req.ProductId,
		).Scan(&p.ProductId, &p.Name, &p.Price)

		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "product %s not found", req.ProductId)
		}
		if err != nil {
			return nil, status.Error(codes.Internal, "could not retrieve product")
		}
		return p, nil
	}

	productsMu.RLock()
	defer productsMu.RUnlock()
	p, exists := products[req.ProductId]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "product %s not found", req.ProductId)
	}
	copyOfProduct := *p
	return &copyOfProduct, nil
}

func (s *productServer) ListProducts(ctx context.Context, req *pb.ListProductsRequest) (*pb.ListProductsResponse, error) {
	result := &pb.ListProductsResponse{}

	if s.db != nil {
		rows, err := s.db.QueryContext(ctx,
			`SELECT product_id, name, price FROM products ORDER BY product_id`)
		if err != nil {
			return nil, status.Error(codes.Internal, "could not list products")
		}
		defer rows.Close()

		for rows.Next() {
			p := &pb.Product{}
			if err := rows.Scan(&p.ProductId, &p.Name, &p.Price); err != nil {
				return nil, status.Error(codes.Internal, "could not read products")
			}
			result.Products = append(result.Products, p)
		}
		if err := rows.Err(); err != nil {
			return nil, status.Error(codes.Internal, "could not read products")
		}
		return result, nil
	}

	productsMu.RLock()
	defer productsMu.RUnlock()
	for _, p := range products {
		copyOfProduct := *p
		result.Products = append(result.Products, &copyOfProduct)
	}
	return result, nil
}

func (s *productServer) UpdateProduct(ctx context.Context, req *pb.UpdateProductRequest) (*pb.Product, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateProduct(req.ProductId, req.Name, req.Price); err != nil {
		return nil, err
	}

	p := &pb.Product{
		ProductId: strings.TrimSpace(req.ProductId),
		Name:      strings.TrimSpace(req.Name),
		Price:     req.Price,
	}

	if s.db != nil {
		result, err := s.db.ExecContext(ctx,
			`UPDATE products SET name = $1, price = $2 WHERE product_id = $3`,
			p.Name, p.Price, p.ProductId,
		)
		if err != nil {
			return nil, status.Error(codes.Internal, "could not update product")
		}
		count, err := result.RowsAffected()
		if err != nil {
			return nil, status.Error(codes.Internal, "could not confirm product update")
		}
		if count == 0 {
			return nil, status.Errorf(codes.NotFound, "product %s not found", p.ProductId)
		}
		return p, nil
	}

	productsMu.Lock()
	defer productsMu.Unlock()
	if _, exists := products[p.ProductId]; !exists {
		return nil, status.Errorf(codes.NotFound, "product %s not found", p.ProductId)
	}
	products[p.ProductId] = p
	return p, nil
}

func (s *productServer) DeleteProduct(ctx context.Context, req *pb.DeleteProductRequest) (*pb.DeleteProductResponse, error) {
	if req == nil || strings.TrimSpace(req.ProductId) == "" {
		return nil, status.Error(codes.InvalidArgument, "product id must not be empty")
	}

	if s.db != nil {
		result, err := s.db.ExecContext(ctx,
			`DELETE FROM products WHERE product_id = $1`, req.ProductId)
		if err != nil {
			return nil, status.Error(codes.Internal, "could not delete product")
		}
		count, err := result.RowsAffected()
		if err != nil {
			return nil, status.Error(codes.Internal, "could not confirm product deletion")
		}
		if count == 0 {
			return nil, status.Errorf(codes.NotFound, "product %s not found", req.ProductId)
		}
		return &pb.DeleteProductResponse{Message: "product deleted"}, nil
	}

	productsMu.Lock()
	defer productsMu.Unlock()
	if _, exists := products[req.ProductId]; !exists {
		return nil, status.Errorf(codes.NotFound, "product %s not found", req.ProductId)
	}
	delete(products, req.ProductId)
	return &pb.DeleteProductResponse{Message: "product deleted"}, nil
}

func connectDatabase() (*sql.DB, error) {
	host := envOrDefault("DB_HOST", "localhost")
	port := envOrDefault("DB_PORT", "5432")
	user := envOrDefault("DB_USER", "postgres")
	password := envOrDefault("DB_PASSWORD", "postgres")
	dbName := envOrDefault("DB_NAME", "products")

	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbName,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS products (
			product_id VARCHAR(50) PRIMARY KEY,
			name VARCHAR(200) NOT NULL,
			price DOUBLE PRECISION NOT NULL CHECK (price > 0)
		)
	`)
	if err != nil {
		db.Close()
		return nil, err
	}

	seeds := []pb.Product{
		{ProductId: "P001", Name: "Laptop", Price: 75000},
		{ProductId: "P002", Name: "Mechanical Keyboard", Price: 4500},
		{ProductId: "P003", Name: "Wireless Mouse", Price: 1800},
		{ProductId: "P004", Name: "Monitor", Price: 25000},
	}

	for _, p := range seeds {
		_, err := db.ExecContext(ctx,
			`INSERT INTO products (product_id, name, price)
			 VALUES ($1, $2, $3) ON CONFLICT (product_id) DO NOTHING`,
			p.ProductId, p.Name, p.Price,
		)
		if err != nil {
			db.Close()
			return nil, err
		}
	}

	return db, nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func main() {
	db, err := connectDatabase()
	if err != nil {
		log.Fatalf("Product Service database connection failed: %v", err)
	}
	defer db.Close()

	port := envOrDefault("PORT", "50051")
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterProductServiceServer(grpcServer, &productServer{db: db})

	log.Printf("Product Service listening on :%s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
