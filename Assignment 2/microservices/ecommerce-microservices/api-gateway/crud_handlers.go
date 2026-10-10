package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"ecommerce-microservices/pkg/resilience"
	pb "ecommerce-microservices/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	orderClient  pb.OrderServiceClient
	orderBreaker = resilience.NewCircuitBreaker(3, 5*time.Second)
)

func gatewayEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func dialGatewayService(key, fallback string) *grpc.ClientConn {
	target := gatewayEnv(key, fallback)
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		panic("could not create connection to " + target + ": " + err.Error())
	}
	return conn
}

func writeProtoJSON(w http.ResponseWriter, code int, message proto.Message) {
	data, err := protojson.Marshal(message)
	if err != nil {
		writeGrpcError(w, status.Error(codes.Internal, "could not encode response"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(data)
	_, _ = w.Write([]byte("\n"))
}

func decodeRESTBody(r *http.Request, target any) error {
	if r.Body == nil {
		return status.Error(codes.InvalidArgument, "request body is required")
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return status.Error(codes.InvalidArgument, "invalid JSON request body")
	}
	return nil
}

func resourceID(r *http.Request, prefix string) string {
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, prefix), "/")
	if id == "" {
		id = r.URL.Query().Get("id")
	}
	return id
}

func resilientGatewayCall(
	breaker *resilience.CircuitBreaker,
	fn func(context.Context) error,
) error {
	return resilientCall(breaker, fn)
}

// PRODUCT CRUD

func productsRESTHandler(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r, "/api/products")

	switch r.Method {
	case http.MethodPost:
		if id != "" {
			http.Error(w, "POST must use /api/products without an ID", http.StatusBadRequest)
			return
		}
		var body struct {
			ProductID string  `json:"productId"`
			Name      string  `json:"name"`
			Price     float64 `json:"price"`
		}
		if err := decodeRESTBody(r, &body); err != nil {
			writeGrpcError(w, err)
			return
		}
		var result *pb.Product
		err := resilientGatewayCall(productBreaker, func(ctx context.Context) error {
			var callErr error
			result, callErr = productClient.CreateProduct(ctx, &pb.CreateProductRequest{
				ProductId: body.ProductID,
				Name:      body.Name,
				Price:     body.Price,
			})
			return callErr
		})
		if err != nil {
			writeGrpcError(w, err)
			return
		}
		writeProtoJSON(w, http.StatusCreated, result)

	case http.MethodGet:
		if id != "" {
			result, err := fetchProduct(productClient, productBreaker, id)
			if err != nil {
				writeGrpcError(w, err)
				return
			}
			writeProtoJSON(w, http.StatusOK, result)
			return
		}

		var result *pb.ListProductsResponse
		err := resilientGatewayCall(productBreaker, func(ctx context.Context) error {
			var callErr error
			result, callErr = productClient.ListProducts(ctx, &pb.ListProductsRequest{})
			return callErr
		})
		if err != nil {
			writeGrpcError(w, err)
			return
		}
		writeProtoJSON(w, http.StatusOK, result)

	case http.MethodPut:
		if id == "" {
			http.Error(w, "product ID is required in the URL", http.StatusBadRequest)
			return
		}
		var body struct {
			Name  string  `json:"name"`
			Price float64 `json:"price"`
		}
		if err := decodeRESTBody(r, &body); err != nil {
			writeGrpcError(w, err)
			return
		}
		var result *pb.Product
		err := resilientGatewayCall(productBreaker, func(ctx context.Context) error {
			var callErr error
			result, callErr = productClient.UpdateProduct(ctx, &pb.UpdateProductRequest{
				ProductId: id,
				Name:      body.Name,
				Price:     body.Price,
			})
			return callErr
		})
		if err != nil {
			writeGrpcError(w, err)
			return
		}
		writeProtoJSON(w, http.StatusOK, result)

	case http.MethodDelete:
		if id == "" {
			http.Error(w, "product ID is required in the URL", http.StatusBadRequest)
			return
		}
		var result *pb.DeleteProductResponse
		err := resilientGatewayCall(productBreaker, func(ctx context.Context) error {
			var callErr error
			result, callErr = productClient.DeleteProduct(ctx, &pb.DeleteProductRequest{
				ProductId: id,
			})
			return callErr
		})
		if err != nil {
			writeGrpcError(w, err)
			return
		}
		writeProtoJSON(w, http.StatusOK, result)

	default:
		w.Header().Set("Allow", "GET, POST, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ORDER CRUD

func ordersRESTHandler(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r, "/api/orders")

	switch r.Method {
	case http.MethodPost:
		if id != "" {
			http.Error(w, "POST must use /api/orders without an ID", http.StatusBadRequest)
			return
		}
		var body struct {
			OrderID   string `json:"orderId"`
			ProductID string `json:"productId"`
			Quantity  int32  `json:"quantity"`
		}
		if err := decodeRESTBody(r, &body); err != nil {
			writeGrpcError(w, err)
			return
		}
		var result *pb.Order
		err := resilientGatewayCall(orderBreaker, func(ctx context.Context) error {
			var callErr error
			result, callErr = orderClient.CreateOrder(ctx, &pb.CreateOrderRequest{
				OrderId:   body.OrderID,
				ProductId: body.ProductID,
				Quantity:  body.Quantity,
			})
			return callErr
		})
		if err != nil {
			writeGrpcError(w, err)
			return
		}
		writeProtoJSON(w, http.StatusCreated, result)

	case http.MethodGet:
		if id != "" {
			var result *pb.Order
			err := resilientGatewayCall(orderBreaker, func(ctx context.Context) error {
				var callErr error
				result, callErr = orderClient.GetOrder(ctx, &pb.GetOrderRequest{
					OrderId: id,
				})
				return callErr
			})
			if err != nil {
				writeGrpcError(w, err)
				return
			}
			writeProtoJSON(w, http.StatusOK, result)
			return
		}

		var result *pb.ListOrdersResponse
		err := resilientGatewayCall(orderBreaker, func(ctx context.Context) error {
			var callErr error
			result, callErr = orderClient.ListOrders(ctx, &pb.ListOrdersRequest{})
			return callErr
		})
		if err != nil {
			writeGrpcError(w, err)
			return
		}
		writeProtoJSON(w, http.StatusOK, result)

	case http.MethodPut:
		if id == "" {
			http.Error(w, "order ID is required in the URL", http.StatusBadRequest)
			return
		}
		var body struct {
			ProductID string `json:"productId"`
			Quantity  int32  `json:"quantity"`
		}
		if err := decodeRESTBody(r, &body); err != nil {
			writeGrpcError(w, err)
			return
		}
		var result *pb.Order
		err := resilientGatewayCall(orderBreaker, func(ctx context.Context) error {
			var callErr error
			result, callErr = orderClient.UpdateOrder(ctx, &pb.UpdateOrderRequest{
				OrderId:   id,
				ProductId: body.ProductID,
				Quantity:  body.Quantity,
			})
			return callErr
		})
		if err != nil {
			writeGrpcError(w, err)
			return
		}
		writeProtoJSON(w, http.StatusOK, result)

	case http.MethodDelete:
		if id == "" {
			http.Error(w, "order ID is required in the URL", http.StatusBadRequest)
			return
		}
		var result *pb.DeleteOrderResponse
		err := resilientGatewayCall(orderBreaker, func(ctx context.Context) error {
			var callErr error
			result, callErr = orderClient.DeleteOrder(ctx, &pb.DeleteOrderRequest{
				OrderId: id,
			})
			return callErr
		})
		if err != nil {
			writeGrpcError(w, err)
			return
		}
		writeProtoJSON(w, http.StatusOK, result)

	default:
		w.Header().Set("Allow", "GET, POST, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
