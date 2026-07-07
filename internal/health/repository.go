package health

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Repository interface {
	CheckDB(ctx context.Context) error
	CheckRedis(ctx context.Context) error
}

type repository struct {
	db  *sql.DB
	rdb *redis.Client
}

func NewRepository(db *sql.DB, rdb *redis.Client) Repository {
	return &repository{
		db:  db,
		rdb: rdb,
	}
}

func (r *repository) CheckDB(ctx context.Context) error {
	if r.db == nil {
		return fmt.Errorf("database connection is not initialized")
	}
	return r.db.PingContext(ctx)
}

func (r *repository) CheckRedis(ctx context.Context) error {
	if r.rdb == nil {
		return fmt.Errorf("redis connection is not initialized")
	}
	return r.rdb.Ping(ctx).Err()
}
