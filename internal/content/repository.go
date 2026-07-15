package content

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

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
	SaveSemanticVectors(ctx context.Context, points []*pb.PointStruct) error
	SearchVectors(ctx context.Context, vec []float32, limit uint32) ([]*pb.ScoredPoint, error)
	SearchSemanticVectors(ctx context.Context, vec []float32, limit uint32) ([]*pb.ScoredPoint, error)
	SearchVectorsWithFilter(ctx context.Context, vec []float32, limit uint32, pointType string) ([]*pb.ScoredPoint, error)
	SearchVectorsBatch(ctx context.Context, vecs [][]float32, limit uint32, pointType string) ([][]*pb.ScoredPoint, error)
	SearchSemanticVectorsBatch(ctx context.Context, vecs [][]float32, limit uint32, pointType string) ([][]*pb.ScoredPoint, error)
	CountSegments(ctx context.Context, parentSha256, pointType string) (int, error)
	SaveSegmentCache(ctx context.Context, key string, result *SegmentVerificationResult) error
	GetSegmentCache(ctx context.Context, key string) (*SegmentVerificationResult, error)
	GetCheckpoint(ctx context.Context, key string) (uint64, error)
	SaveCheckpoint(ctx context.Context, key string, val uint64) error
	GetLineage(ctx context.Context, hash string) ([]*database.ContentRecord, error)
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
	INSERT INTO content_records (sha256_hash, creator_address, phash, timestamp, ipfs_cid, ai_tool, media_ipfs_url, media_s3_url, allow_ai_training, media_type, webhook_url, parent_sha256)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	ON CONFLICT (sha256_hash) DO NOTHING;`

	_, err := r.db.ExecContext(ctx, query,
		record.Sha256Hash, record.CreatorAddress, record.PHash, record.Timestamp, record.IpfsCid, record.AiTool,
		record.MediaIpfsUrl, record.MediaS3Url, record.AllowAiTraining, record.MediaType, record.WebhookUrl, record.ParentSha256,
	)
	return err
}

func (r *repository) GetPostgres(ctx context.Context, hash string) (*database.ContentRecord, error) {
	query := `
	SELECT sha256_hash, creator_address, phash, timestamp, ipfs_cid, ai_tool, media_ipfs_url, media_s3_url, allow_ai_training, media_type, webhook_url, COALESCE(parent_sha256, '') as parent_sha256
	FROM content_records
	WHERE sha256_hash = $1;`

	var record database.ContentRecord
	err := r.db.QueryRowContext(ctx, query, hash).Scan(
		&record.Sha256Hash, &record.CreatorAddress, &record.PHash, &record.Timestamp, &record.IpfsCid, &record.AiTool,
		&record.MediaIpfsUrl, &record.MediaS3Url, &record.AllowAiTraining, &record.MediaType, &record.WebhookUrl, &record.ParentSha256,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (r *repository) GetLineage(ctx context.Context, hash string) ([]*database.ContentRecord, error) {
	query := `
	WITH RECURSIVE lineage AS (
		SELECT sha256_hash, creator_address, phash, timestamp, ipfs_cid, ai_tool, media_ipfs_url, media_s3_url, allow_ai_training, media_type, webhook_url, COALESCE(parent_sha256, '') as parent_sha256
		FROM content_records WHERE sha256_hash = $1
		UNION ALL
		SELECT cr.sha256_hash, cr.creator_address, cr.phash, cr.timestamp, cr.ipfs_cid, cr.ai_tool, cr.media_ipfs_url, cr.media_s3_url, cr.allow_ai_training, cr.media_type, cr.webhook_url, COALESCE(cr.parent_sha256, '') as parent_sha256
		FROM content_records cr
		INNER JOIN lineage l ON cr.sha256_hash = l.parent_sha256
		WHERE cr.sha256_hash != ''
	)
	SELECT * FROM lineage;`

	rows, err := r.db.QueryContext(ctx, query, hash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*database.ContentRecord
	for rows.Next() {
		var record database.ContentRecord
		if err := rows.Scan(
			&record.Sha256Hash, &record.CreatorAddress, &record.PHash, &record.Timestamp, &record.IpfsCid, &record.AiTool,
			&record.MediaIpfsUrl, &record.MediaS3Url, &record.AllowAiTraining, &record.MediaType, &record.WebhookUrl, &record.ParentSha256,
		); err != nil {
			return nil, err
		}
		records = append(records, &record)
	}
	return records, rows.Err()
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

func (r *repository) SaveSemanticVectors(ctx context.Context, points []*pb.PointStruct) error {
	if len(points) == 0 {
		return nil
	}
	_, err := r.qdrant.Points.Upsert(ctx, &pb.UpsertPoints{
		CollectionName: "veritrace_semantics",
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

func (r *repository) SearchSemanticVectors(ctx context.Context, vec []float32, limit uint32) ([]*pb.ScoredPoint, error) {
	if len(vec) == 0 {
		return nil, nil
	}
	resp, err := r.qdrant.Points.Search(ctx, &pb.SearchPoints{
		CollectionName: "veritrace_semantics",
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

func (r *repository) SearchVectorsWithFilter(ctx context.Context, vec []float32, limit uint32, pointType string) ([]*pb.ScoredPoint, error) {
	resp, err := r.qdrant.Points.Search(ctx, &pb.SearchPoints{
		CollectionName: "veritrace_signatures",
		Vector:         vec,
		Limit:          uint64(limit),
		WithPayload: &pb.WithPayloadSelector{
			SelectorOptions: &pb.WithPayloadSelector_Enable{
				Enable: true,
			},
		},
		Filter: &pb.Filter{
			Must: []*pb.Condition{
				{
					ConditionOneOf: &pb.Condition_Field{
						Field: &pb.FieldCondition{
							Key: "point_type",
							Match: &pb.Match{
								MatchValue: &pb.Match_Keyword{
									Keyword: pointType,
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetResult(), nil
}

func (r *repository) SearchVectorsBatch(ctx context.Context, vecs [][]float32, limit uint32, pointType string) ([][]*pb.ScoredPoint, error) {
	var searchPoints []*pb.SearchPoints
	for _, v := range vecs {
		searchPoints = append(searchPoints, &pb.SearchPoints{
			CollectionName: "veritrace_signatures",
			Vector:         v,
			Limit:          uint64(limit),
			Filter: &pb.Filter{
				Must: []*pb.Condition{
					{
						ConditionOneOf: &pb.Condition_Field{
							Field: &pb.FieldCondition{
								Key: "point_type",
								Match: &pb.Match{
									MatchValue: &pb.Match_Keyword{
										Keyword: pointType,
									},
								},
							},
						},
					},
				},
			},
			WithPayload: &pb.WithPayloadSelector{
				SelectorOptions: &pb.WithPayloadSelector_Enable{
					Enable: true,
				},
			},
		})
	}
	resp, err := r.qdrant.Points.SearchBatch(ctx, &pb.SearchBatchPoints{
		CollectionName: "veritrace_signatures",
		SearchPoints:   searchPoints,
	})
	if err != nil {
		return nil, err
	}
	var results [][]*pb.ScoredPoint
	for _, batchResult := range resp.GetResult() {
		results = append(results, batchResult.GetResult())
	}
	return results, nil
}

func (r *repository) SearchSemanticVectorsBatch(ctx context.Context, vecs [][]float32, limit uint32, pointType string) ([][]*pb.ScoredPoint, error) {
	if len(vecs) == 0 {
		return nil, nil
	}
	var searchPoints []*pb.SearchPoints
	for _, v := range vecs {
		if len(v) == 0 {
			continue
		}
		searchPoints = append(searchPoints, &pb.SearchPoints{
			CollectionName: "veritrace_semantics",
			Vector:         v,
			Limit:          uint64(limit),
			Filter: &pb.Filter{
				Must: []*pb.Condition{
					{
						ConditionOneOf: &pb.Condition_Field{
							Field: &pb.FieldCondition{
								Key: "point_type",
								Match: &pb.Match{
									MatchValue: &pb.Match_Keyword{
										Keyword: pointType,
									},
								},
							},
						},
					},
				},
			},
			WithPayload: &pb.WithPayloadSelector{
				SelectorOptions: &pb.WithPayloadSelector_Enable{
					Enable: true,
				},
			},
		})
	}
	if len(searchPoints) == 0 {
		return nil, nil
	}
	resp, err := r.qdrant.Points.SearchBatch(ctx, &pb.SearchBatchPoints{
		CollectionName: "veritrace_semantics",
		SearchPoints:   searchPoints,
	})
	if err != nil {
		return nil, err
	}
	var results [][]*pb.ScoredPoint
	for _, batchResult := range resp.GetResult() {
		results = append(results, batchResult.GetResult())
	}
	return results, nil
}

func (r *repository) CountSegments(ctx context.Context, parentSha256, pointType string) (int, error) {
	resp, err := r.qdrant.Points.Count(ctx, &pb.CountPoints{
		CollectionName: "veritrace_signatures",
		Filter: &pb.Filter{
			Must: []*pb.Condition{
				{
					ConditionOneOf: &pb.Condition_Field{
						Field: &pb.FieldCondition{
							Key: "parent_sha256",
							Match: &pb.Match{
								MatchValue: &pb.Match_Keyword{
									Keyword: parentSha256,
								},
							},
						},
					},
				},
				{
					ConditionOneOf: &pb.Condition_Field{
						Field: &pb.FieldCondition{
							Key: "point_type",
							Match: &pb.Match{
								MatchValue: &pb.Match_Keyword{
									Keyword: pointType,
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		return 0, err
	}
	return int(resp.GetResult().GetCount()), nil
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

func (r *repository) SaveSegmentCache(ctx context.Context, key string, result *SegmentVerificationResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, "seg:"+key, data, time.Hour).Err()
}

func (r *repository) GetSegmentCache(ctx context.Context, key string) (*SegmentVerificationResult, error) {
	val, err := r.rdb.Get(ctx, "seg:"+key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var result SegmentVerificationResult
	if err := json.Unmarshal([]byte(val), &result); err != nil {
		return nil, err
	}
	return &result, nil
}
