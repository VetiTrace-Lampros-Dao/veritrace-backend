package main

import (
	"context"
	"fmt"
	"log"

	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial("localhost:6334", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	pointsClient := qdrant.NewPointsClient(conn)

	// Create a dummy vector of 64 elements all 1.0
	vec := make([]float32, 64)
	for i := 0; i < 64; i++ {
		vec[i] = 1.0
	}

	resp, err := pointsClient.Search(context.Background(), &qdrant.SearchPoints{
		CollectionName: "veritrace_signatures",
		Vector:         vec,
		Limit:          1,
	})
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}

	for _, result := range resp.GetResult() {
		fmt.Printf("Score: %f\n", result.GetScore())
	}
}
