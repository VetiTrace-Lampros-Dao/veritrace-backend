package content

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/database"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/onchain"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/webhook"
	pb "github.com/qdrant/go-client/qdrant"
)

type KeyframePayload struct {
	Offset       uint64      `json:"offset"`
	PHash        uint64      `json:"phash"`
	SemanticHash []float32   `json:"semantic_hash,omitempty"`
	FaceHashes   [][]float32 `json:"face_hashes,omitempty"`
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
	IsDeepfake              bool                    `json:"is_deepfake"`
	IsAudioDeepfake         bool                    `json:"is_audio_deepfake"`
}

type VerificationCertificate struct {
	CertificateID   string `json:"certificate_id"`
	IssuedAt        string `json:"issued_at"`
	TargetHash      string `json:"target_hash"`
	MatchFound      bool   `json:"match_found"`
	OriginalCreator string `json:"original_creator,omitempty"`
	OnChainTxHash   string `json:"on_chain_tx_hash,omitempty"`
	IpfsCid         string `json:"ipfs_cid,omitempty"`
	Signature       string `json:"signature"`
}

type Service interface {
	Register(ctx context.Context, record database.ContentRecord, keyframes []KeyframePayload, mediaType string, rootSemanticHash []float32, rootFaceHashes [][]float32, rootAudioHash []float32) error
	VerifyExact(ctx context.Context, hash string) (*VerificationResult, error)
	VerifyFuzzy(ctx context.Context, phash uint64) (*VerificationResult, error)
	VerifySegments(ctx context.Context, sha256 string, segments []KeyframePayload, mediaType string, audioHash []float32) (*SegmentVerificationResult, error)
	GenerateCertificate(ctx context.Context, hash string) (*VerificationCertificate, error)
	PinToIPFS(ctx context.Context, payload interface{}) (string, error)
	PinFile(ctx context.Context, reader io.Reader, filename, contentType string) (string, string, error)
	GetCheckpoint(ctx context.Context, key string) (uint64, error)
	SaveCheckpoint(ctx context.Context, key string, val uint64) error
	GetLineage(ctx context.Context, hash string) ([]*database.ContentRecord, error)
}

type service struct {
	repo            Repository
	cfg             *config.Config
	storage         StorageProvider
	onchainVerifier *onchain.Verifier
	dispatcher      webhook.Dispatcher
}

func NewService(repo Repository, cfg *config.Config, storage StorageProvider, onchainVerifier *onchain.Verifier, dispatcher webhook.Dispatcher) Service {
	return &service{
		repo:            repo,
		cfg:             cfg,
		storage:         storage,
		onchainVerifier: onchainVerifier,
		dispatcher:      dispatcher,
	}
}

func (s *service) GetCheckpoint(ctx context.Context, key string) (uint64, error) {
	return s.repo.GetCheckpoint(ctx, key)
}

func (s *service) SaveCheckpoint(ctx context.Context, key string, val uint64) error {
	return s.repo.SaveCheckpoint(ctx, key, val)
}

func (s *service) Register(ctx context.Context, record database.ContentRecord, keyframes []KeyframePayload, mediaType string, rootSemanticHash []float32, rootFaceHashes [][]float32, rootAudioHash []float32) error {
	if err := s.repo.SavePostgres(ctx, record); err != nil {
		return fmt.Errorf("failed to save to postgres: %w", err)
	}

	if err := s.repo.SaveCache(ctx, record); err != nil {
		log.Printf("Service warning: failed to write cache: %v", err)
	}

	var points []*pb.PointStruct
	var semPoints []*pb.PointStruct

	var facePoints []*pb.PointStruct
	var audioPoints []*pb.PointStruct

	if mediaType == "document" {
		points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, record.PHash, 0, mediaType, "document"))
		if sp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, rootSemanticHash, 0, mediaType, "document"); sp != nil {
			semPoints = append(semPoints, sp)
		}
		for _, fh := range rootFaceHashes {
			if fp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, fh, 0, mediaType, "document"); fp != nil {
				facePoints = append(facePoints, fp)
			}
		}
		for _, kf := range keyframes {
			points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, kf.PHash, kf.Offset, mediaType, "page"))
			if sp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, kf.SemanticHash, kf.Offset, mediaType, "page"); sp != nil {
				semPoints = append(semPoints, sp)
			}
			for _, fh := range kf.FaceHashes {
				if fp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, fh, kf.Offset, mediaType, "page"); fp != nil {
					facePoints = append(facePoints, fp)
				}
			}
		}
	} else if mediaType == "video" && len(keyframes) > 0 {
		if ap := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, rootAudioHash, 0, mediaType, "video"); ap != nil {
			audioPoints = append(audioPoints, ap)
		}
		for _, kf := range keyframes {
			points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, kf.PHash, kf.Offset, mediaType, "keyframe"))
			if sp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, kf.SemanticHash, kf.Offset, mediaType, "keyframe"); sp != nil {
				semPoints = append(semPoints, sp)
			}
			for _, fh := range kf.FaceHashes {
				if fp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, fh, kf.Offset, mediaType, "keyframe"); fp != nil {
					facePoints = append(facePoints, fp)
				}
			}
		}
	} else {
		points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, record.PHash, 0, mediaType, "image"))
		if sp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, rootSemanticHash, 0, mediaType, "image"); sp != nil {
			semPoints = append(semPoints, sp)
		}
		for _, fh := range rootFaceHashes {
			if fp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, fh, 0, mediaType, "image"); fp != nil {
				facePoints = append(facePoints, fp)
			}
		}
	}

	if err := s.repo.SaveVectors(ctx, points); err != nil {
		return fmt.Errorf("failed to index vectors: %w", err)
	}
	if err := s.repo.SaveSemanticVectors(ctx, semPoints); err != nil {
		return fmt.Errorf("failed to index semantic vectors: %w", err)
	}
	if len(facePoints) > 0 {
		if err := s.repo.SaveFaceVectors(ctx, facePoints); err != nil {
			log.Printf("Failed to index face vectors: %v", err)
		}
	}
	if len(audioPoints) > 0 {
		if err := s.repo.SaveAudioVectors(ctx, audioPoints); err != nil {
			log.Printf("Failed to index audio vectors: %v", err)
		}
	}

	return nil
}

func (s *service) VerifyExact(ctx context.Context, hash string) (*VerificationResult, error) {
	cached, err := s.repo.GetCache(ctx, hash)
	if err == nil && cached != nil {
		verified, txHash := s.crossCheckBlockchain(ctx, hash, cached.IpfsCid)

		if cached.WebhookUrl != "" {
			s.dispatcher.Dispatch(cached.WebhookUrl, webhook.Payload{
				EventType:    webhook.EventMatchDetected,
				OriginalHash: cached.Sha256Hash,
				Similarity:   100.0,
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
				Message:      fmt.Sprintf("An exact match of your registered asset (%s) was verified.", cached.Sha256Hash),
			})
		}

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

	verified, txHash := s.crossCheckBlockchain(ctx, record.Sha256Hash, record.IpfsCid)

	if record.WebhookUrl != "" {
		s.dispatcher.Dispatch(record.WebhookUrl, webhook.Payload{
			EventType:    webhook.EventMatchDetected,
			OriginalHash: record.Sha256Hash,
			Similarity:   100.0,
			Timestamp:    time.Now().UTC().Format(time.RFC3339),
			Message:      fmt.Sprintf("An exact match of your registered asset (%s) was verified.", record.Sha256Hash),
		})
	}

	return &VerificationResult{
		MatchFound:      true,
		ExactMatch:      true,
		Similarity:      100.0,
		Record:          record,
		OnChainVerified: verified,
		OnChainTxHash:   txHash,
	}, nil
}

func (s *service) GenerateCertificate(ctx context.Context, hash string) (*VerificationCertificate, error) {
	result, err := s.VerifyExact(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to verify record: %w", err)
	}

	if !result.MatchFound {
		return nil, fmt.Errorf("no match found for hash %s, cannot generate certificate", hash)
	}

	cert := &VerificationCertificate{
		CertificateID:   generateUUID(),
		IssuedAt:        time.Now().UTC().Format(time.RFC3339),
		TargetHash:      hash,
		MatchFound:      true,
		OriginalCreator: result.Record.CreatorAddress,
		OnChainTxHash:   result.OnChainTxHash,
		IpfsCid:         result.Record.IpfsCid,
	}

	// Sign the certificate contents
	h := hmac.New(sha256.New, []byte("veritrace-secret-key-2026"))
	h.Write([]byte(fmt.Sprintf("%s:%s:%s:%s", cert.CertificateID, cert.TargetHash, cert.OriginalCreator, cert.OnChainTxHash)))
	cert.Signature = hex.EncodeToString(h.Sum(nil))

	return cert, nil
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

	threshold := 22.0

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

	if recordResult.Record.WebhookUrl != "" {
		s.dispatcher.Dispatch(recordResult.Record.WebhookUrl, webhook.Payload{
			EventType:    webhook.EventDerivativeDetected,
			OriginalHash: recordResult.Record.Sha256Hash,
			Similarity:   similarity,
			Timestamp:    time.Now().UTC().Format(time.RFC3339),
			Message:      fmt.Sprintf("A derivative copy of your registered asset (%s) was verified.", recordResult.Record.Sha256Hash),
		})
	}

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

func (s *service) VerifySegments(ctx context.Context, sha256 string, segments []KeyframePayload, mediaType string, audioHash []float32) (*SegmentVerificationResult, error) {
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
	threshold := 22.0
	semanticThreshold := 0.85

	vecs := make([][]float32, len(segments))
	semVecs := make([][]float32, len(segments))
	var faceVecs [][]float32 // Flattened array of all faces from all segments
	
	for i, seg := range segments {
		vecs[i] = phashToVector(seg.PHash)
		semVecs[i] = seg.SemanticHash
		for _, fh := range seg.FaceHashes {
			faceVecs = append(faceVecs, fh)
		}
	}

	batchResults, err := s.repo.SearchVectorsBatch(ctx, vecs, 1, pt)
	if err != nil {
		log.Printf("SearchVectorsBatch failed: %v", err)
	}

	semBatchResults, err := s.repo.SearchSemanticVectorsBatch(ctx, semVecs, 1, pt)
	if err != nil {
		log.Printf("SearchSemanticVectorsBatch failed: %v", err)
	}
	
	var faceBatchResults [][]*pb.ScoredPoint
	if len(faceVecs) > 0 {
		faceBatchResults, err = s.repo.SearchFaceVectorsBatch(ctx, faceVecs, 1, pt)
		if err != nil {
			log.Printf("SearchFaceVectorsBatch failed: %v", err)
		}
	}

	var audioBatchResults [][]*pb.ScoredPoint
	if len(audioHash) > 0 {
		// Only one audio hash per video
		audioBatchResults, err = s.repo.SearchAudioVectorsBatch(ctx, [][]float32{audioHash}, 1, "video")
		if err != nil {
			log.Printf("SearchAudioVectorsBatch failed: %v", err)
		}
	}

	matchCounts := make(map[string]int)
	for _, results := range batchResults {
		if len(results) == 0 {
			continue
		}
		top := results[0]
		if float64(top.GetScore()) > threshold {
			continue
		}
		payload := top.GetPayload()
		if payload == nil {
			continue
		}
		parentVal, ok := payload["parent_sha256"]
		if ok {
			matchCounts[parentVal.GetStringValue()]++
		}
	}

	semMatchCounts := make(map[string]int)
	for _, results := range semBatchResults {
		if len(results) == 0 {
			continue
		}
		top := results[0]
		if float64(top.GetScore()) < semanticThreshold {
			continue
		}
		payload := top.GetPayload()
		if payload == nil {
			continue
		}
		parentVal, ok := payload["parent_sha256"]
		if ok {
			semMatchCounts[parentVal.GetStringValue()]++
		}
	}

	faceMatchCounts := make(map[string]int)
	faceThreshold := 0.6 // Cosine similarity threshold for ArcFace
	for _, results := range faceBatchResults {
		if len(results) == 0 {
			continue
		}
		top := results[0]
		if float64(top.GetScore()) < faceThreshold {
			continue
		}
		payload := top.GetPayload()
		if payload == nil {
			continue
		}
		parentVal, ok := payload["parent_sha256"]
		if ok {
			faceMatchCounts[parentVal.GetStringValue()]++
		}
	}

	bestParent := ""
	bestCount := 0
	for parent, count := range matchCounts {
		if count > bestCount {
			bestCount = count
			bestParent = parent
		}
	}

	bestSemParent := ""
	bestSemCount := 0
	for parent, count := range semMatchCounts {
		if count > bestSemCount {
			bestSemCount = count
			bestSemParent = parent
		}
	}

	bestFaceParent := ""
	bestFaceCount := 0
	for parent, count := range faceMatchCounts {
		if count > bestFaceCount {
			bestFaceCount = count
			bestFaceParent = parent
		}
	}

	audioMatchCounts := make(map[string]int)
	audioThreshold := 0.85 // Cosine similarity threshold for audio
	for _, results := range audioBatchResults {
		if len(results) == 0 {
			continue
		}
		top := results[0]
		if float64(top.GetScore()) < audioThreshold {
			continue
		}
		payload := top.GetPayload()
		if payload == nil {
			continue
		}
		parentVal, ok := payload["parent_sha256"]
		if ok {
			audioMatchCounts[parentVal.GetStringValue()]++
		}
	}

	coverageUploadedPct := 0.0
	if len(segments) > 0 {
		coverageUploadedPct = float64(bestCount) / float64(len(segments)) * 100.0
	}

	semCoverageUploadedPct := 0.0
	if len(segments) > 0 {
		semCoverageUploadedPct = float64(bestSemCount) / float64(len(segments)) * 100.0
	}
	
	faceCoverageUploadedPct := 0.0
	if len(segments) > 0 {
		faceCoverageUploadedPct = float64(bestFaceCount) / float64(len(segments)) * 100.0
	}

	isDeepfake := false
	isAudioDeepfake := false
	finalParent := ""
	finalCoveragePct := 0.0
	finalMatchedSegments := 0

	if coverageUploadedPct >= 5.0 {
		finalParent = bestParent
		finalCoveragePct = coverageUploadedPct
		finalMatchedSegments = bestCount
		// If visual matches completely, but audio hash differs -> Audio deepfake!
		if len(audioHash) > 0 && audioMatchCounts[finalParent] == 0 {
			isDeepfake = true
			isAudioDeepfake = true
		}
		finalParent = bestParent
		finalCoveragePct = coverageUploadedPct
		finalMatchedSegments = bestCount
	} else if faceCoverageUploadedPct >= 10.0 {
		finalParent = bestFaceParent
		finalCoveragePct = faceCoverageUploadedPct
		finalMatchedSegments = bestFaceCount
		isDeepfake = true
	} else if semCoverageUploadedPct >= 10.0 {
		finalParent = bestSemParent
		finalCoveragePct = semCoverageUploadedPct
		finalMatchedSegments = bestSemCount
		isDeepfake = true
	} else {
		return &SegmentVerificationResult{MatchFound: false}, nil
	}

	parentResult, err := s.VerifyExact(ctx, finalParent)
	if err != nil || !parentResult.MatchFound {
		return &SegmentVerificationResult{MatchFound: false}, nil
	}

	totalRegistered, _ := s.repo.CountSegments(ctx, finalParent, pt)
	coverageRegisteredPct := 0.0
	if totalRegistered > 0 {
		coverageRegisteredPct = float64(finalMatchedSegments) / float64(totalRegistered) * 100.0
	}

	similarity := (finalCoveragePct + coverageRegisteredPct) / 2.0
	
	// Penalize the similarity score if the video matches but the audio was swapped
	if isAudioDeepfake {
		similarity = similarity * 0.5
	}

	verified, txHash := s.crossCheckBlockchain(ctx, parentResult.Record.Sha256Hash, parentResult.Record.IpfsCid)

	if parentResult.Record.WebhookUrl != "" {
		msg := fmt.Sprintf("A matching segment of your registered asset (%s) was verified.", parentResult.Record.Sha256Hash)
		if isAudioDeepfake {
			msg = fmt.Sprintf("ALERT: An Audio Deepfake (Voice Cloning) of your registered video (%s) was detected! The audio track has been manipulated.", parentResult.Record.Sha256Hash)
		} else if isDeepfake {
			msg = fmt.Sprintf("ALERT: A Deepfake/AI-altered version of your registered asset (%s) was detected!", parentResult.Record.Sha256Hash)
		}
		s.dispatcher.Dispatch(parentResult.Record.WebhookUrl, webhook.Payload{
			EventType:    webhook.EventDerivativeDetected,
			OriginalHash: parentResult.Record.Sha256Hash,
			Similarity:   similarity,
			Timestamp:    time.Now().UTC().Format(time.RFC3339),
			Message:      msg,
		})
	}

	result := &SegmentVerificationResult{
		MatchFound:              true,
		ExactMatch:              false,
		Similarity:              similarity,
		MatchedSegments:         finalMatchedSegments,
		TotalSegmentsUploaded:   len(segments),
		TotalSegmentsRegistered: totalRegistered,
		CoverageUploadedPct:     finalCoveragePct,
		CoverageRegisteredPct:   coverageRegisteredPct,
		Record:                  parentResult.Record,
		OnChainVerified:         verified,
		OnChainTxHash:           txHash,
		IsDeepfake:              isDeepfake,
		IsAudioDeepfake:         isAudioDeepfake,
	}

	_ = s.repo.SaveSegmentCache(ctx, cacheKey, result)

	return result, nil
}

func segmentPointType(mediaType string) string {
	if mediaType == "video" {
		return "keyframe"
	} else if mediaType == "document" {
		return "page"
	}
	return "image"
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

func (s *service) buildSemanticPoint(sha256, creator string, semanticHash []float32, offset uint64, mediaType, pointType string) *pb.PointStruct {
	if len(semanticHash) == 0 {
		return nil
	}
	uuidStr := generateUUID()

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
		Vectors: pb.NewVectorsDense(semanticHash),
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

func (s *service) GetLineage(ctx context.Context, hash string) ([]*database.ContentRecord, error) {
	return s.repo.GetLineage(ctx, hash)
}
