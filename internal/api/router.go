package api

import (
	"database/sql"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/content"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/health"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/vector"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func SetupRouter(db *sql.DB, rdb *redis.Client, qdrant *vector.QdrantClient) *gin.Engine {
	r := gin.Default()

	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	healthRepo := health.NewRepository(db, rdb)
	healthService := health.NewService(healthRepo)
	healthHandler := health.NewHandler(healthService)

	contentRepo := content.NewRepository(db, rdb, qdrant)
	contentService := content.NewService(contentRepo)
	contentHandler := content.NewHandler(contentService)

	r.GET("/health", healthHandler.CheckHealth)

	r.GET("/api/v1/verify/exact", contentHandler.VerifyExact)
	r.GET("/api/v1/verify/fuzzy", contentHandler.VerifyFuzzy)

	return r
}
