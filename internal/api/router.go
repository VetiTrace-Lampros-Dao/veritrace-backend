package api

import (
	"database/sql"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/health"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func SetupRouter(db *sql.DB, rdb *redis.Client) *gin.Engine {
	r := gin.Default()

	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	healthRepo := health.NewRepository(db, rdb)
	healthService := health.NewService(healthRepo)
	healthHandler := health.NewHandler(healthService)

	r.GET("/health", healthHandler.CheckHealth)

	return r
}
