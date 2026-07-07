package health

import (
	"context"
	"log"
)

type HealthStatus struct {
	Status     string `json:"status"`
	Database   string `json:"database"`
	Redis      string `json:"redis"`
	DBError    string `json:"db_error,omitempty"`
	RedisError string `json:"redis_error,omitempty"`
}

type Service interface {
	CheckHealth(ctx context.Context) HealthStatus
}

type service struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &service{
		repo: repo,
	}
}

func (s *service) CheckHealth(ctx context.Context) HealthStatus {
	status := HealthStatus{
		Status:   "UP",
		Database: "UP",
		Redis:    "UP",
	}

	if err := s.repo.CheckDB(ctx); err != nil {
		log.Printf("Health Check: DB check failed: %v", err)
		status.Database = "DOWN"
		status.DBError = err.Error()
		status.Status = "DOWN"
	}

	if err := s.repo.CheckRedis(ctx); err != nil {
		log.Printf("Health Check: Redis check failed: %v", err)
		status.Redis = "DOWN"
		status.RedisError = err.Error()
		status.Status = "DOWN"
	}

	return status
}
