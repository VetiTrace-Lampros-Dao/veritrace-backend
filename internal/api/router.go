package api

import (
	"context"
	"database/sql"
	"log"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/content"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/health"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/onchain"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/vector"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/webhook"
	"github.com/gin-gonic/gin"
	pb "github.com/qdrant/go-client/qdrant"
	"github.com/redis/go-redis/v9"
)

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func SetupRouter(db *sql.DB, rdb *redis.Client, qdrant *vector.QdrantClient, cfg *config.Config) *gin.Engine {
	r := gin.Default()

	r.Use(corsMiddleware())
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	r.Static("/uploads", "./uploads")

	healthRepo := health.NewRepository(db, rdb)
	healthService := health.NewService(healthRepo)
	healthHandler := health.NewHandler(healthService)

	storage, err := content.InitStorageProvider(context.Background(), cfg)
	if err != nil {
		log.Printf("Router warning: failed to initialize storage provider: %v", err)
	}

	onchainVerifier, err := onchain.NewVerifier(cfg)
	if err != nil {
		log.Printf("Router warning: failed to initialize onchain verifier: %v", err)
	}

	contentRepo := content.NewRepository(db, rdb, qdrant)
	dispatcher := webhook.NewDispatcher()
	contentService := content.NewService(contentRepo, cfg, storage, onchainVerifier, dispatcher)
	contentHandler := content.NewHandler(contentService)

	enterpriseHandler := NewEnterpriseHandler(db, qdrant)

	r.GET("/health", healthHandler.CheckHealth)

	r.GET("/api/v1/verify/exact", contentHandler.VerifyExact)
	r.GET("/api/v1/verify/certificate", contentHandler.ExportCertificate)
	r.GET("/api/v1/verify/fuzzy", contentHandler.VerifyFuzzy)
	r.POST("/api/v1/verify/segments", contentHandler.VerifySegments)
	r.POST("/api/v1/verify/flag", contentHandler.FlagContent)
	r.POST("/api/v1/pin", contentHandler.PinToIPFS)
	r.POST("/api/v1/pin-file", contentHandler.PinFile)
	r.GET("/api/v1/content/:hash/lineage", contentHandler.GetLineage)
	
	// Enterprise endpoints
	r.GET("/api/v1/enterprise/dataset", enterpriseHandler.QueryDataset)
	r.POST("/api/v1/enterprise/unlock", enterpriseHandler.UnlockDataset)

	r.POST("/api/v1/dev/flush", func(c *gin.Context) {
		_, err := db.Exec("TRUNCATE TABLE content_records, sync_checkpoints RESTART IDENTITY;")
		if err != nil {
			c.JSON(500, gin.H{"error": "failed to truncate postgres: " + err.Error()})
			return
		}

		err = rdb.FlushAll(c.Request.Context()).Err()
		if err != nil {
			c.JSON(500, gin.H{"error": "failed to flush redis: " + err.Error()})
			return
		}

		_, err = qdrant.Points.Delete(c.Request.Context(), &pb.DeletePoints{
			CollectionName: "veritrace_signatures",
			Points: &pb.PointsSelector{
				PointsSelectorOneOf: &pb.PointsSelector_Filter{
					Filter: &pb.Filter{},
				},
			},
		})
		if err != nil {
			log.Printf("Dev Flush: Points delete failed (%v), recreating collection...", err)
			_, _ = qdrant.Collections.Delete(c.Request.Context(), &pb.DeleteCollection{
				CollectionName: "veritrace_signatures",
			})
			_, err = qdrant.Collections.Create(c.Request.Context(), &pb.CreateCollection{
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
				c.JSON(500, gin.H{"error": "failed to recreate qdrant collection: " + err.Error()})
				return
			}
		}

		c.JSON(200, gin.H{"status": "success", "message": "all data successfully flushed"})
	})

	return r
}

