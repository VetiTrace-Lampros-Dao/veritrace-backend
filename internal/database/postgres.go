package database

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
)

type ContentRecord struct {
	Sha256Hash      string
	CreatorAddress  string
	PHash           uint64
	Timestamp       uint64
	IpfsCid         string
	AiTool          string
	MediaIpfsUrl    string
	MediaS3Url      string
	AllowAiTraining bool
	MediaType       string
}

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

	var pingErr error
	for i := 0; i < 5; i++ {
		pingErr = db.Ping()
		if pingErr == nil {
			log.Println("Successfully connected to PostgreSQL database")
			break
		}
		log.Printf("Failed to ping PostgreSQL database (attempt %d/5): %v. Retrying in 2 seconds...\n", i+1, pingErr)
		time.Sleep(2 * time.Second)
	}

	if pingErr != nil {
		return nil, fmt.Errorf("database connection failed after retries: %w", pingErr)
	}

	if err := RunMigrations(db); err != nil {
		return nil, fmt.Errorf("failed to run database migrations: %w", err)
	}

	return db, nil
}

func RunMigrations(db *sql.DB) error {
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://migrations",
		"postgres",
		driver,
	)
	if err != nil {
		return fmt.Errorf("failed to initialize migrate: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	log.Println("Database migrations applied successfully")
	return nil
}
