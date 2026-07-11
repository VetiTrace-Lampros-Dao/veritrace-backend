package vector

import (
	"context"
	"fmt"
	"time"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type QdrantClient struct {
	Conn        *grpc.ClientConn
	Collections pb.CollectionsClient
	Points      pb.PointsClient
}

func InitQdrant(cfg *config.Config) (*QdrantClient, error) {
	addr := fmt.Sprintf("%s:%s", cfg.QdrantHost, cfg.QdrantPort)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Qdrant: %w", err)
	}

	collectionsClient := pb.NewCollectionsClient(conn)
	pointsClient := pb.NewPointsClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listResp, err := collectionsClient.List(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to list Qdrant collections: %w", err)
	}

	exists := false
	for _, coll := range listResp.GetCollections() {
		if coll.GetName() == "veritrace_signatures" {
			exists = true
			break
		}
	}

	if !exists {
		_, err = collectionsClient.Create(ctx, &pb.CreateCollection{
			CollectionName: "veritrace_signatures",
			VectorsConfig: &pb.VectorsConfig{
				Config: &pb.VectorsConfig_Params{
					Params: &pb.VectorParams{
						Size:     64,
						Distance: pb.Distance_Manhattan,
					},
				},
			},
		})
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to create collection: %w", err)
		}
	}

	_, err = pointsClient.CreateFieldIndex(ctx, &pb.CreateFieldIndexCollection{
		CollectionName: "veritrace_signatures",
		FieldName:      "parent_sha256",
		FieldType:      pb.FieldType_FieldTypeKeyword.Enum(),
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create field index parent_sha256: %w", err)
	}

	_, err = pointsClient.CreateFieldIndex(ctx, &pb.CreateFieldIndexCollection{
		CollectionName: "veritrace_signatures",
		FieldName:      "point_type",
		FieldType:      pb.FieldType_FieldTypeKeyword.Enum(),
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create field index point_type: %w", err)
	}

	return &QdrantClient{
		Conn:        conn,
		Collections: collectionsClient,
		Points:      pointsClient,
	}, nil
}

func (q *QdrantClient) Close() error {
	if q.Conn != nil {
		return q.Conn.Close()
	}
	return nil
}
