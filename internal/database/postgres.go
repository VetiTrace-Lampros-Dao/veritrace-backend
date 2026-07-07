package database

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	_ "github.com/lib/pq"
)

func ConnectPostgres(cfg *config.Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName, cfg.DBSslMode)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Printf("Warning: Failed to ping PostgreSQL database: %v\n", err)
	} else {
		log.Println("Successfully connected to PostgreSQL database")
	}

	return db, nil
}
