package api

import (
	"database/sql"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/content"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/health"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/vector"
	"github.com/gin-gonic/gin"
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

	healthRepo := health.NewRepository(db, rdb)
	healthService := health.NewService(healthRepo)
	healthHandler := health.NewHandler(healthService)

	contentRepo := content.NewRepository(db, rdb, qdrant)
	contentService := content.NewService(contentRepo, cfg)
	contentHandler := content.NewHandler(contentService)

	r.GET("/health", healthHandler.CheckHealth)

	r.GET("/api/v1/verify/exact", contentHandler.VerifyExact)
	r.GET("/api/v1/verify/fuzzy", contentHandler.VerifyFuzzy)
	r.POST("/api/v1/pin", contentHandler.PinToIPFS)

	return r
}

