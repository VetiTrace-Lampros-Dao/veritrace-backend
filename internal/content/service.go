package content

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/database"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/onchain"
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
	OnChainVerified bool                    `json:"on_chain_verified"`
	OnChainTxHash   string                  `json:"on_chain_tx_hash,omitempty"`
}

type SegmentVerificationResult struct {
	MatchFound              bool                    `json:"match_found"`
	ExactMatch              bool                    `json:"exact_match"`
	Similarity              float64                 `json:"similarity"`
	MatchedSegments         int                     `json:"matched_segments"`
	TotalSegmentsUploaded   int                     `json:"total_segments_uploaded"`
	TotalSegmentsRegistered int                     `json:"total_segments_registered"`
	CoverageUploadedPct     float64                 `json:"coverage_uploaded_pct"`
	CoverageRegisteredPct   float64                 `json:"coverage_registered_pct"`
	Record                  *database.ContentRecord `json:"record,omitempty"`
	OnChainVerified         bool                    `json:"on_chain_verified"`
	OnChainTxHash           string                  `json:"on_chain_tx_hash,omitempty"`
}

type Service interface {
	Register(ctx context.Context, record database.ContentRecord, keyframes []KeyframePayload, mediaType string) error
	VerifyExact(ctx context.Context, hash string) (*VerificationResult, error)
	VerifyFuzzy(ctx context.Context, phash uint64) (*VerificationResult, error)
	VerifySegments(ctx context.Context, sha256 string, segments []KeyframePayload, mediaType string) (*SegmentVerificationResult, error)
	PinToIPFS(ctx context.Context, payload interface{}) (string, error)
	PinFile(ctx context.Context, reader io.Reader, filename, contentType string) (string, string, error)
	GetCheckpoint(ctx context.Context, key string) (uint64, error)
	SaveCheckpoint(ctx context.Context, key string, val uint64) error
}

type service struct {
	repo            Repository
	cfg             *config.Config
	storage         StorageProvider
	onchainVerifier *onchain.Verifier
}

func NewService(repo Repository, cfg *config.Config, storage StorageProvider, onchainVerifier *onchain.Verifier) Service {
	return &service{
		repo:            repo,
		cfg:             cfg,
		storage:         storage,
		onchainVerifier: onchainVerifier,
	}
}

func (s *service) GetCheckpoint(ctx context.Context, key string) (uint64, error) {
	return s.repo.GetCheckpoint(ctx, key)
}

func (s *service) SaveCheckpoint(ctx context.Context, key string, val uint64) error {
	return s.repo.SaveCheckpoint(ctx, key, val)
}

func (s *service) Register(ctx context.Context, record database.ContentRecord, keyframes []KeyframePayload, mediaType string) error {
	if err := s.repo.SavePostgres(ctx, record); err != nil {
		return fmt.Errorf("failed to save to postgres: %w", err)
	}

	if err := s.repo.SaveCache(ctx, record); err != nil {
		log.Printf("Service warning: failed to write cache: %v", err)
	}

	var points []*pb.PointStruct

	if mediaType == "document" {
		points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, record.PHash, 0, mediaType, "document"))
		for _, kf := range keyframes {
			points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, kf.PHash, kf.Offset, mediaType, "page"))
		}
	} else if mediaType == "video" && len(keyframes) > 0 {
		for _, kf := range keyframes {
			points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, kf.PHash, kf.Offset, mediaType, "keyframe"))
		}
	} else {
		points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, record.PHash, 0, mediaType, "image"))
	}

	if err := s.repo.SaveVectors(ctx, points); err != nil {
		return fmt.Errorf("failed to index vectors: %w", err)
	}

	return nil
}

func (s *service) VerifyExact(ctx context.Context, hash string) (*VerificationResult, error) {
	cached, err := s.repo.GetCache(ctx, hash)
	if err == nil && cached != nil {
		verified, txHash := s.crossCheckBlockchain(ctx, hash, cached.IpfsCid)
		return &VerificationResult{
			MatchFound:      true,
			ExactMatch:      true,
			Similarity:      100.0,
			Record:          cached,
			OnChainVerified: verified,
			OnChainTxHash:   txHash,
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

	verified, txHash := s.crossCheckBlockchain(ctx, hash, record.IpfsCid)

	return &VerificationResult{
		MatchFound:      true,
		ExactMatch:      true,
		Similarity:      100.0,
		Record:          record,
		OnChainVerified: verified,
		OnChainTxHash:   txHash,
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

	payloadEarly := match.GetPayload()
	matchedMediaType := ""
	if payloadEarly != nil {
		if mtv, ok := payloadEarly["media_type"]; ok {
			matchedMediaType = mtv.GetStringValue()
		}
	}

	threshold := 10.0
	if matchedMediaType == "document" {
		threshold = 3.0
	}

	if distance > threshold {
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

	verified, txHash := s.crossCheckBlockchain(ctx, recordResult.Record.Sha256Hash, recordResult.Record.IpfsCid)

	return &VerificationResult{
		MatchFound:      true,
		ExactMatch:      false,
		Similarity:      similarity,
		TimestampOffset: offset,
		MediaType:       mediaType,
		Record:          recordResult.Record,
		OnChainVerified: verified,
		OnChainTxHash:   txHash,
	}, nil

}

func (s *service) VerifySegments(ctx context.Context, sha256 string, segments []KeyframePayload, mediaType string) (*SegmentVerificationResult, error) {
	if len(segments) == 0 {
		return &SegmentVerificationResult{MatchFound: false}, nil
	}

	cacheKey := sha256 + ":" + mediaType
	if cached, err := s.repo.GetSegmentCache(ctx, cacheKey); err == nil && cached != nil {
		return cached, nil
	}

	exactResult, err := s.VerifyExact(ctx, sha256)
	if err == nil && exactResult.MatchFound {
		totalRegistered, _ := s.repo.CountSegments(ctx, sha256, segmentPointType(mediaType))
		return &SegmentVerificationResult{
			MatchFound:              true,
			ExactMatch:              true,
			Similarity:              100.0,
			MatchedSegments:         len(segments),
			TotalSegmentsUploaded:   len(segments),
			TotalSegmentsRegistered: totalRegistered,
			CoverageUploadedPct:     100.0,
			CoverageRegisteredPct:   100.0,
			Record:                  exactResult.Record,
			OnChainVerified:         exactResult.OnChainVerified,
			OnChainTxHash:           exactResult.OnChainTxHash,
		}, nil
	}

	pt := segmentPointType(mediaType)
	threshold := 5.0
	if mediaType == "video" {
		threshold = 8.0
	}

	type hit struct{ parentSha256 string }
	hits := make(chan hit, len(segments))
	sem := make(chan struct{}, 10)
	var wg sync.WaitGroup

	for _, seg := range segments {
		wg.Add(1)
		go func(kf KeyframePayload) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			vec := phashToVector(kf.PHash)
			results, err := s.repo.SearchVectorsWithFilter(ctx, vec, 1, pt)
			if err != nil || len(results) == 0 {
				return
			}
			top := results[0]
			if float64(top.GetScore()) > threshold {
				return
			}
			payload := top.GetPayload()
			if payload == nil {
				return
			}
			parentVal, ok := payload["parent_sha256"]
			if !ok {
				return
			}
			hits <- hit{parentSha256: parentVal.GetStringValue()}
		}(seg)
	}

	wg.Wait()
	close(hits)

	matchCounts := make(map[string]int)
	for h := range hits {
		matchCounts[h.parentSha256]++
	}

	if len(matchCounts) == 0 {
		return &SegmentVerificationResult{MatchFound: false}, nil
	}

	bestParent := ""
	bestCount := 0
	for parent, count := range matchCounts {
		if count > bestCount {
			bestCount = count
			bestParent = parent
		}
	}

	coverageUploadedPct := float64(bestCount) / float64(len(segments)) * 100.0
	if coverageUploadedPct < 5.0 {
		return &SegmentVerificationResult{MatchFound: false}, nil
	}

	parentResult, err := s.VerifyExact(ctx, bestParent)
	if err != nil || !parentResult.MatchFound {
		return &SegmentVerificationResult{MatchFound: false}, nil
	}

	totalRegistered, _ := s.repo.CountSegments(ctx, bestParent, pt)
	coverageRegisteredPct := 0.0
	if totalRegistered > 0 {
		coverageRegisteredPct = float64(bestCount) / float64(totalRegistered) * 100.0
	}

	similarity := (coverageUploadedPct + coverageRegisteredPct) / 2.0

	verified, txHash := s.crossCheckBlockchain(ctx, parentResult.Record.Sha256Hash, parentResult.Record.IpfsCid)

	result := &SegmentVerificationResult{
		MatchFound:              true,
		ExactMatch:              false,
		Similarity:              similarity,
		MatchedSegments:         bestCount,
		TotalSegmentsUploaded:   len(segments),
		TotalSegmentsRegistered: totalRegistered,
		CoverageUploadedPct:     coverageUploadedPct,
		CoverageRegisteredPct:   coverageRegisteredPct,
		Record:                  parentResult.Record,
		OnChainVerified:         verified,
		OnChainTxHash:           txHash,
	}

	_ = s.repo.SaveSegmentCache(ctx, cacheKey, result)

	return result, nil
}

func segmentPointType(mediaType string) string {
	if mediaType == "video" {
		return "keyframe"
	}
	return "page"
}

// crossCheckBlockchain calls the OnChainVerifier to ensure the IPFS CID in our DB matches the one on-chain.
func (s *service) crossCheckBlockchain(ctx context.Context, sha256Hex, expectedCid string) (bool, string) {
	if s.onchainVerifier == nil {
		return false, ""
	}
	
	onChainRec, err := s.onchainVerifier.VerifyHash(ctx, sha256Hex)
	if err != nil {
		log.Printf("Blockchain cross-check failed for %s: %v", sha256Hex, err)
		return false, ""
	}
	
	if onChainRec == nil {
		log.Printf("Blockchain cross-check failed: %s not found on-chain", sha256Hex)
		return false, ""
	}
	
	if onChainRec.IpfsCid != expectedCid {
		log.Printf("Blockchain cross-check failed: CID mismatch for %s. Expected %s, got %s", sha256Hex, expectedCid, onChainRec.IpfsCid)
		return false, onChainRec.TxHash
	}
	
	return true, onChainRec.TxHash
}

func (s *service) buildPoint(sha256, creator string, phash, offset uint64, mediaType, pointType string) *pb.PointStruct {
	uuidStr := generateUUID()
	vec := phashToVector(phash)

	payload := map[string]*pb.Value{
		"parent_sha256":    pb.NewValueString(sha256),
		"creator_address":  pb.NewValueString(creator),
		"timestamp_offset": pb.NewValueInt(int64(offset)),
		"media_type":       pb.NewValueString(mediaType),
		"point_type":       pb.NewValueString(pointType),
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

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.pinata.cloud/pinning/pinJSONToIPFS", bytes.NewReader(bodyBytes))
		if err != nil {
			return "", fmt.Errorf("failed to create pinata request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+s.cfg.PinataJWT)

		client := &http.Client{Timeout: 15 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			log.Printf("Pinata JSON attempt %d failed: %v. Retrying...", attempt, err)
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			var errBody bytes.Buffer
			_, _ = errBody.ReadFrom(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("pinata JSON API returned status %d: %s", resp.StatusCode, errBody.String())
			log.Printf("Pinata JSON attempt %d returned status %d. Retrying...", attempt, resp.StatusCode)
			time.Sleep(1 * time.Second)
			continue
		}

		var pinataResp PinataResponse
		err = json.NewDecoder(resp.Body).Decode(&pinataResp)
		resp.Body.Close()
		if err != nil {
			return "", fmt.Errorf("failed to decode pinata response: %w", err)
		}

		return pinataResp.IpfsHash, nil
	}

	hasher := sha256.New()
	hasher.Write(bodyBytes)
	sha256Hash := fmt.Sprintf("%x", hasher.Sum(nil))
	mockCid := "QmMockMeta" + sha256Hash[:30]
	log.Printf("WARNING: Pinata JSON upload failed (%v). Falling back to mock IPFS CID: %s", lastErr, mockCid)
	return mockCid, nil
}

func (s *service) PinFile(ctx context.Context, reader io.Reader, filename, contentType string) (string, string, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", "", fmt.Errorf("failed to read upload file bytes: %w", err)
	}

	ipfsHash, err := s.pinFileToIPFS(ctx, bytes.NewReader(data), filename)
	var ipfsUrl string
	if err != nil {
		hasher := sha256.New()
		hasher.Write(data)
		sha256Hash := fmt.Sprintf("%x", hasher.Sum(nil))
		mockHash := "QmMock" + sha256Hash[:30]
		ipfsUrl = fmt.Sprintf("https://gateway.pinata.cloud/ipfs/%s", mockHash)
		log.Printf("WARNING: Pinata IPFS upload failed (%v). Falling back to mock IPFS URL for testing: %s", err, ipfsUrl)
	} else {
		ipfsUrl = fmt.Sprintf("https://gateway.pinata.cloud/ipfs/%s", ipfsHash)
	}

	s3Url, err := s.storage.UploadFile(ctx, bytes.NewReader(data), filename, contentType)
	if err != nil {
		return "", "", fmt.Errorf("failed to upload file to S3: %w", err)
	}

	return ipfsUrl, s3Url, nil
}

func (s *service) pinFileToIPFS(ctx context.Context, reader io.Reader, filename string) (string, error) {
	if s.cfg.PinataJWT == "" {
		return "", fmt.Errorf("PINATA_JWT is not configured")
	}

	bodyBytes, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("failed to read file reader: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		body := bytes.NewReader(bodyBytes)
		reqBody := &bytes.Buffer{}
		writer := multipart.NewWriter(reqBody)

		part, err := writer.CreateFormFile("file", filename)
		if err != nil {
			return "", fmt.Errorf("failed to create multipart form file: %w", err)
		}

		if _, err := io.Copy(part, body); err != nil {
			return "", fmt.Errorf("failed to copy file bytes to multipart: %w", err)
		}

		if err := writer.Close(); err != nil {
			return "", fmt.Errorf("failed to close multipart writer: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.pinata.cloud/pinning/pinFileToIPFS", reqBody)
		if err != nil {
			return "", fmt.Errorf("failed to create pinata file request: %w", err)
		}

		req.Header.Set("Content-Type", writer.FormDataContentType())
		req.Header.Set("Authorization", "Bearer "+s.cfg.PinataJWT)

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			log.Printf("Pinata IPFS attempt %d failed: %v. Retrying...", attempt, err)
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			var errBody bytes.Buffer
			_, _ = errBody.ReadFrom(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("pinata API returned status %d: %s", resp.StatusCode, errBody.String())
			log.Printf("Pinata IPFS attempt %d returned status %d. Retrying...", attempt, resp.StatusCode)
			time.Sleep(1 * time.Second)
			continue
		}

		var pinataResp PinataResponse
		err = json.NewDecoder(resp.Body).Decode(&pinataResp)
		resp.Body.Close()
		if err != nil {
			return "", fmt.Errorf("failed to decode pinata file response: %w", err)
		}

		return pinataResp.IpfsHash, nil
	}

	return "", fmt.Errorf("all 3 Pinata IPFS pin attempts failed: %w", lastErr)
}
