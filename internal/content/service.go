package content

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/database"
	pb "github.com/qdrant/go-client/qdrant"
)

type KeyframePayload struct {
	Offset uint64 `json:"offset"`
	PHash  uint64 `json:"phash"`
}

type VerificationResult struct {
	MatchFound      bool                    `json:"match_found"`
	ExactMatch      bool                    `json:"exact_match"`
	Similarity      float64                 `json:"similarity"`
	TimestampOffset uint64                  `json:"timestamp_offset,omitempty"`
	MediaType       string                  `json:"media_type,omitempty"`
	Record          *database.ContentRecord `json:"record,omitempty"`
}

type Service interface {
	Register(ctx context.Context, record database.ContentRecord, keyframes []KeyframePayload, mediaType string) error
	VerifyExact(ctx context.Context, hash string) (*VerificationResult, error)
	VerifyFuzzy(ctx context.Context, phash uint64) (*VerificationResult, error)
	PinToIPFS(ctx context.Context, payload interface{}) (string, error)
}

type service struct {
	repo Repository
	cfg  *config.Config
}

func NewService(repo Repository, cfg *config.Config) Service {
	return &service{
		repo: repo,
		cfg:  cfg,
	}
}

func (s *service) Register(ctx context.Context, record database.ContentRecord, keyframes []KeyframePayload, mediaType string) error {
	if err := s.repo.SavePostgres(ctx, record); err != nil {
		return fmt.Errorf("failed to save to postgres: %w", err)
	}

	if err := s.repo.SaveCache(ctx, record); err != nil {
		log.Printf("Service warning: failed to write cache: %v", err)
	}

	var points []*pb.PointStruct
	if len(keyframes) > 0 {
		for _, kf := range keyframes {
			points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, kf.PHash, kf.Offset, mediaType))
		}
	} else {
		points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, record.PHash, 0, mediaType))
	}

	if err := s.repo.SaveVectors(ctx, points); err != nil {
		return fmt.Errorf("failed to index vectors: %w", err)
	}


	return nil
}

func (s *service) VerifyExact(ctx context.Context, hash string) (*VerificationResult, error) {
	cached, err := s.repo.GetCache(ctx, hash)
	if err == nil && cached != nil {
		return &VerificationResult{
			MatchFound: true,
			ExactMatch: true,
			Similarity: 100.0,
			Record:     cached,
		}, nil
	}

	record, err := s.repo.GetPostgres(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to query database: %w", err)
	}

	if record == nil {
		return &VerificationResult{
			MatchFound: false,
		}, nil
	}

	_ = s.repo.SaveCache(ctx, *record)

	return &VerificationResult{
		MatchFound: true,
		ExactMatch: true,
		Similarity: 100.0,
		Record:     record,
	}, nil
}

func (s *service) VerifyFuzzy(ctx context.Context, phash uint64) (*VerificationResult, error) {
	vec := phashToVector(phash)
	matches, err := s.repo.SearchVectors(ctx, vec, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to search vectors: %w", err)
	}

	if len(matches) == 0 {
		return &VerificationResult{
			MatchFound: false,
		}, nil
	}

	match := matches[0]
	distance := float64(match.GetScore())

	if distance > 10.0 {
		return &VerificationResult{
			MatchFound: false,
		}, nil
	}

	payload := match.GetPayload()
	if payload == nil {
		return &VerificationResult{
			MatchFound: false,
		}, nil
	}

	parentHashVal, ok := payload["parent_sha256"]
	if !ok {
		return &VerificationResult{
			MatchFound: false,
		}, nil
	}

	parentHash := parentHashVal.GetStringValue()
	offsetVal, ok := payload["timestamp_offset"]
	var offset uint64
	if ok {
		offset = uint64(offsetVal.GetIntegerValue())
	}

	mediaTypeVal, ok := payload["media_type"]
	var mediaType string
	if ok {
		mediaType = mediaTypeVal.GetStringValue()
	}

	recordResult, err := s.VerifyExact(ctx, parentHash)
	if err != nil || !recordResult.MatchFound {
		return &VerificationResult{
			MatchFound: false,
		}, nil
	}

	similarity := ((64.0 - distance) / 64.0) * 100.0

	return &VerificationResult{
		MatchFound:      true,
		ExactMatch:      false,
		Similarity:      similarity,
		TimestampOffset: offset,
		MediaType:       mediaType,
		Record:          recordResult.Record,
	}, nil

}

func (s *service) buildPoint(sha256, creator string, phash, offset uint64, mediaType string) *pb.PointStruct {
	uuidStr := generateUUID()
	vec := phashToVector(phash)

	payload := map[string]*pb.Value{
		"parent_sha256":    pb.NewValueString(sha256),
		"creator_address":  pb.NewValueString(creator),
		"timestamp_offset": pb.NewValueInt(int64(offset)),
		"media_type":       pb.NewValueString(mediaType),
	}

	return &pb.PointStruct{
		Id: &pb.PointId{
			PointIdOptions: &pb.PointId_Uuid{
				Uuid: uuidStr,
			},
		},
		Vectors: pb.NewVectorsDense(vec),
		Payload: payload,
	}
}

func phashToVector(phash uint64) []float32 {
	vec := make([]float32, 64)
	for i := 0; i < 64; i++ {
		if (phash & (1 << uint(i))) != 0 {
			vec[i] = 1.0
		} else {
			vec[i] = 0.0
		}
	}
	return vec
}

func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type PinataMetadata struct {
	Name string `json:"name"`
}

type PinataPayload struct {
	PinataContent  interface{}    `json:"pinataContent"`
	PinataMetadata PinataMetadata `json:"pinataMetadata"`
}

type PinataResponse struct {
	IpfsHash  string    `json:"IpfsHash"`
	PinSize   int64     `json:"PinSize"`
	Timestamp time.Time `json:"Timestamp"`
}

func (s *service) PinToIPFS(ctx context.Context, payload interface{}) (string, error) {
	if s.cfg.PinataJWT == "" {
		return "", fmt.Errorf("PINATA_JWT is not configured")
	}

	pinataPayload := PinataPayload{
		PinataContent: payload,
		PinataMetadata: PinataMetadata{
			Name: fmt.Sprintf("veritrace-metadata-%d.json", time.Now().Unix()),
		},
	}

	bodyBytes, err := json.Marshal(pinataPayload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal pinata payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.pinata.cloud/pinning/pinJSONToIPFS", bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("failed to create pinata request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.cfg.PinataJWT)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to execute pinata request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errBody bytes.Buffer
		_, _ = errBody.ReadFrom(resp.Body)
		return "", fmt.Errorf("pinata API returned status %d: %s", resp.StatusCode, errBody.String())
	}

	var pinataResp PinataResponse
	if err := json.NewDecoder(resp.Body).Decode(&pinataResp); err != nil {
		return "", fmt.Errorf("failed to decode pinata response: %w", err)
	}

	return pinataResp.IpfsHash, nil
}

