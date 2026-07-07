package main

import (
	"context"
	"log"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/api"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/content"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/database"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/listener"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/vector"
)

func main() {
	log.Println("Starting Veritrace Backend Server...")

	cfg := config.LoadConfig()

	db, err := database.ConnectPostgres(cfg)
	if err != nil {
		log.Fatalf("Critical error connecting to database: %v", err)
	}
	defer func() {
		if db != nil {
			if err := db.Close(); err != nil {
				log.Printf("Error closing database connection: %v\n", err)
			}
		}
	}()

	rdb, err := database.ConnectRedis(cfg)
	if err != nil {
		log.Fatalf("Critical error connecting to Redis: %v", err)
	}
	defer func() {
		if rdb != nil {
			if err := rdb.Close(); err != nil {
				log.Printf("Error closing Redis connection: %v\n", err)
			}
		}
	}()

	qdrant, err := vector.InitQdrant(cfg)
	if err != nil {
		log.Fatalf("Critical error connecting to Qdrant: %v", err)
	}
	defer func() {
		if qdrant != nil {
			if err := qdrant.Close(); err != nil {
				log.Printf("Error closing Qdrant connection: %v\n", err)
			}
		}
	}()

	contentRepo := content.NewRepository(db, rdb, qdrant)
	contentService := content.NewService(contentRepo)

	evmListener, err := listener.NewEVMListener(cfg)
	if err != nil {
		log.Fatalf("Critical error initializing EVM listener: %v", err)
	}
	defer evmListener.Close()

	pipeline := listener.NewPipeline(cfg, contentService, evmListener)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := evmListener.Start(ctx); err != nil {
		log.Fatalf("Critical error starting EVM listener: %v", err)
	}

	pipeline.Start(ctx, 5)

	r := api.SetupRouter(db, rdb, qdrant)

	log.Printf("Server is running on port %s\n", cfg.Port)
	if err := r.Run(cfg.Port); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}
