package main

import (
	"log"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/api"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/database"
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

	r := api.SetupRouter(db, rdb)

	log.Printf("Server is running on port %s\n", cfg.Port)
	if err := r.Run(cfg.Port); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}
