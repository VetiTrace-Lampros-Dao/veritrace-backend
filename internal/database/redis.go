package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/redis/go-redis/v9"
)

func ConnectRedis(cfg *config.Config) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Failed to ping Redis: %v\n", err)
	} else {
		log.Println("Successfully connected to Redis")
	}

	return rdb, nil
}
