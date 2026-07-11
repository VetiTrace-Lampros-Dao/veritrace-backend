package content

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/database"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/vector"
	pb "github.com/qdrant/go-client/qdrant"
	"github.com/redis/go-redis/v9"
)

type Repository interface {
	SavePostgres(ctx context.Context, record database.ContentRecord) error
	GetPostgres(ctx context.Context, hash string) (*database.ContentRecord, error)
	SaveCache(ctx context.Context, record database.ContentRecord) error
	GetCache(ctx context.Context, hash string) (*database.ContentRecord, error)
	SaveVectors(ctx context.Context, points []*pb.PointStruct) error
	SearchVectors(ctx context.Context, vec []float32, limit uint32) ([]*pb.ScoredPoint, error)
	GetCheckpoint(ctx context.Context, key string) (uint64, error)
	SaveCheckpoint(ctx context.Context, key string, val uint64) error
}

type repository struct {
	db     *sql.DB
	rdb    *redis.Client
	qdrant *vector.QdrantClient
}

func NewRepository(db *sql.DB, rdb *redis.Client, qdrant *vector.QdrantClient) Repository {
	return &repository{
		db:     db,
		rdb:    rdb,
		qdrant: qdrant,
	}
}

func (r *repository) SavePostgres(ctx context.Context, record database.ContentRecord) error {
	query := `
	INSERT INTO content_records (sha256_hash, creator_address, phash, timestamp, ipfs_cid, ai_tool, media_ipfs_url, media_s3_url, allow_ai_training, media_type)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	ON CONFLICT (sha256_hash) DO NOTHING;`

	_, err := r.db.ExecContext(ctx, query, 
		record.Sha256Hash, record.CreatorAddress, record.PHash, record.Timestamp, record.IpfsCid, record.AiTool,
		record.MediaIpfsUrl, record.MediaS3Url, record.AllowAiTraining, record.MediaType,
	)
	return err
}

func (r *repository) GetPostgres(ctx context.Context, hash string) (*database.ContentRecord, error) {
	query := `
	SELECT sha256_hash, creator_address, phash, timestamp, ipfs_cid, ai_tool, media_ipfs_url, media_s3_url, allow_ai_training, media_type
	FROM content_records
	WHERE sha256_hash = $1;`

	var record database.ContentRecord
	err := r.db.QueryRowContext(ctx, query, hash).Scan(
		&record.Sha256Hash, &record.CreatorAddress, &record.PHash, &record.Timestamp, &record.IpfsCid, &record.AiTool,
		&record.MediaIpfsUrl, &record.MediaS3Url, &record.AllowAiTraining, &record.MediaType,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (r *repository) SaveCache(ctx context.Context, record database.ContentRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, record.Sha256Hash, data, 0).Err()
}

func (r *repository) GetCache(ctx context.Context, hash string) (*database.ContentRecord, error) {
	val, err := r.rdb.Get(ctx, hash).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var record database.ContentRecord
	if err := json.Unmarshal([]byte(val), &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (r *repository) SaveVectors(ctx context.Context, points []*pb.PointStruct) error {
	_, err := r.qdrant.Points.Upsert(ctx, &pb.UpsertPoints{
		CollectionName: "veritrace_signatures",
		Points:         points,
	})
	return err
}

func (r *repository) SearchVectors(ctx context.Context, vec []float32, limit uint32) ([]*pb.ScoredPoint, error) {
	resp, err := r.qdrant.Points.Search(ctx, &pb.SearchPoints{
		CollectionName: "veritrace_signatures",
		Vector:         vec,
		Limit:          uint64(limit),
		WithPayload: &pb.WithPayloadSelector{
			SelectorOptions: &pb.WithPayloadSelector_Enable{
				Enable: true,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetResult(), nil
}

func (r *repository) GetCheckpoint(ctx context.Context, key string) (uint64, error) {
	query := `SELECT last_value FROM sync_checkpoints WHERE key = $1;`
	var val uint64
	err := r.db.QueryRowContext(ctx, query, key).Scan(&val)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return val, err
}

func (r *repository) SaveCheckpoint(ctx context.Context, key string, val uint64) error {
	query := `
	INSERT INTO sync_checkpoints (key, last_value)
	VALUES ($1, $2)
	ON CONFLICT (key) DO UPDATE SET last_value = EXCLUDED.last_value;`
	_, err := r.db.ExecContext(ctx, query, key, val)
	return err
}
