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
	"math"
	"mime/multipart"
	"net/http"
	"sort"
	"strings"
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
	Caption      string      `json:"caption,omitempty"`
}

type MatchDetail struct {
	Sha256Hash           string                  `json:"sha256_hash"`
	CreatorAddress       string                  `json:"creator_address"`
	PHash                uint64                  `json:"phash"`
	Similarity           float64                 `json:"similarity"`
	Timestamp            uint64                  `json:"timestamp"`
	MediaType            string                  `json:"media_type"`
	MatchType            string                  `json:"match_type"` // "exact", "similar", "deepfake"
	IsDeepfake           bool                    `json:"is_deepfake"`
	IsAudioDeepfake      bool                    `json:"is_audio_deepfake"`
	TemporalIntegrity    float64                 `json:"temporal_integrity"`
	ConfidenceScore      float64                 `json:"confidence_score"`
	ConfidenceTier       string                  `json:"confidence_tier"`
	MediaIpfsUrl         string                  `json:"media_ipfs_url,omitempty"`
	MediaS3Url           string                  `json:"media_s3_url,omitempty"`
	IpfsCid              string                  `json:"ipfs_cid,omitempty"`
	AiTool               string                  `json:"ai_tool,omitempty"`
	OnChainVerified      bool                    `json:"on_chain_verified"`
	OnChainTxHash        string                  `json:"on_chain_tx_hash,omitempty"`
	MatchedSegments      int                     `json:"matched_segments"`
	FlagCount            int                     `json:"flag_count"`
	PublisherFlagCount   int                     `json:"publisher_flag_count"`
	ConsensusCount       int                     `json:"consensus_count"`
	IsPublisherVerified  bool                    `json:"is_publisher_verified"`
	PublisherName        string                  `json:"publisher_name,omitempty"`
	Record               *database.ContentRecord `json:"record,omitempty"`
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
	Matches         []MatchDetail           `json:"matches,omitempty"`
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
	TemporalIntegrity       float64                 `json:"temporal_integrity"`
	DebugVersion            string                  `json:"debug_version,omitempty"`
	Matches                 []MatchDetail           `json:"matches,omitempty"`
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
	Register(ctx context.Context, record database.ContentRecord, keyframes []KeyframePayload, mediaType string, rootSemanticHash []float32, rootFaceHashes [][]float32, rootAudioHash []float32, caption string) error
	VerifyExact(ctx context.Context, hash string) (*VerificationResult, error)
	VerifyFuzzy(ctx context.Context, phash uint64) (*VerificationResult, error)
	VerifySegments(ctx context.Context, sha256 string, segments []KeyframePayload, mediaType string, audioHash []float32) (*SegmentVerificationResult, error)
	GenerateCertificate(ctx context.Context, hash string) (*VerificationCertificate, error)
	PinToIPFS(ctx context.Context, payload interface{}) (string, error)
	PinFile(ctx context.Context, reader io.Reader, filename, contentType string) (string, string, error)
	GetCheckpoint(ctx context.Context, key string) (uint64, error)
	SaveCheckpoint(ctx context.Context, key string, val uint64) error
	GetLineage(ctx context.Context, hash string) ([]*database.ContentRecord, error)
	FlagContent(ctx context.Context, hash, reporter, reason string, timestamp int64) error
	VerifyPublisherDomain(ctx context.Context, domain, address string) error
	ListVerifiedPublishers(ctx context.Context) ([]database.VerifiedPublisher, error)
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

func (s *service) Register(ctx context.Context, record database.ContentRecord, keyframes []KeyframePayload, mediaType string, rootSemanticHash []float32, rootFaceHashes [][]float32, rootAudioHash []float32, caption string) error {

	var plagiarismCheck *SegmentVerificationResult
	var fuzzyCheck *VerificationResult

	if mediaType == "video" || mediaType == "document" || mediaType == "text" {
		plagiarismCheck, _ = s.VerifySegments(ctx, record.Sha256Hash, keyframes, mediaType, rootAudioHash)
	} else if mediaType == "image" {
		fuzzyCheck, _ = s.VerifyFuzzy(ctx, record.PHash)
	}

	var matchFound bool
	var matchedCreator string
	var matchSimilarity float64
	var matchedHash string
	var matchedWebhook string

	if plagiarismCheck != nil && plagiarismCheck.MatchFound {
		matchFound = true
		matchedCreator = plagiarismCheck.Record.CreatorAddress
		matchSimilarity = plagiarismCheck.Similarity
		matchedHash = plagiarismCheck.Record.Sha256Hash
		matchedWebhook = plagiarismCheck.Record.WebhookUrl
	} else if fuzzyCheck != nil && fuzzyCheck.MatchFound {
		matchFound = true
		matchedCreator = fuzzyCheck.Record.CreatorAddress
		matchSimilarity = fuzzyCheck.Similarity
		matchedHash = fuzzyCheck.Record.Sha256Hash
		matchedWebhook = fuzzyCheck.Record.WebhookUrl
	}

	if matchFound && matchSimilarity >= 80.0 {
		record.ParentSha256 = matchedHash
		if matchedCreator != record.CreatorAddress && matchedWebhook != "" {
			s.dispatcher.Dispatch(matchedWebhook, webhook.Payload{
				EventType:    webhook.EventPlagiarismAlert,
				OriginalHash: matchedHash,
				Similarity:   matchSimilarity,
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
				Message:      fmt.Sprintf("URGENT: A user (%s) has attempted to register content that is %.1f%% similar to your protected asset (%s) on the VeriTrace registry. This could be an attempt to plagiarize your work.", record.CreatorAddress, matchSimilarity, matchedHash),
			})
		}
	}

	// 2. Save to Postgres
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
	var textPoints []*pb.PointStruct

	if mediaType == "document" {
		points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, record.PHash, 0, mediaType, "document"))
		if sp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, rootSemanticHash, 0, mediaType, "document", caption); sp != nil {
			semPoints = append(semPoints, sp)
		}
		for _, fh := range rootFaceHashes {
			if fp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, fh, 0, mediaType, "document", ""); fp != nil {
				facePoints = append(facePoints, fp)
			}
		}
		for _, kf := range keyframes {
			points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, kf.PHash, kf.Offset, mediaType, "page"))
			if sp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, kf.SemanticHash, kf.Offset, mediaType, "page", kf.Caption); sp != nil {
				semPoints = append(semPoints, sp)
			}
			for _, fh := range kf.FaceHashes {
				if fp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, fh, kf.Offset, mediaType, "page", ""); fp != nil {
					facePoints = append(facePoints, fp)
				}
			}
		}
	} else if mediaType == "video" && len(keyframes) > 0 {
		if ap := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, rootAudioHash, 0, mediaType, "video", ""); ap != nil {
			audioPoints = append(audioPoints, ap)
		}
		for _, kf := range keyframes {
			points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, kf.PHash, kf.Offset, mediaType, "keyframe"))
			if sp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, kf.SemanticHash, kf.Offset, mediaType, "keyframe", kf.Caption); sp != nil {
				semPoints = append(semPoints, sp)
			}
			for _, fh := range kf.FaceHashes {
				if fp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, fh, kf.Offset, mediaType, "keyframe", ""); fp != nil {
					facePoints = append(facePoints, fp)
				}
			}
		}
	} else if mediaType == "text" {
		if sp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, rootSemanticHash, 0, mediaType, "text", ""); sp != nil {
			textPoints = append(textPoints, sp)
		}
	} else {
		points = append(points, s.buildPoint(record.Sha256Hash, record.CreatorAddress, record.PHash, 0, mediaType, "image"))
		if sp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, rootSemanticHash, 0, mediaType, "image", caption); sp != nil {
			semPoints = append(semPoints, sp)
		}
		for _, fh := range rootFaceHashes {
			if fp := s.buildSemanticPoint(record.Sha256Hash, record.CreatorAddress, fh, 0, mediaType, "image", ""); fp != nil {
				facePoints = append(facePoints, fp)
			}
		}
	}

	if len(points) > 0 {
		if err := s.repo.SaveVectors(ctx, points); err != nil {
			return fmt.Errorf("failed to index vectors: %w", err)
		}
	}
	if len(semPoints) > 0 {
		if err := s.repo.SaveSemanticVectors(ctx, semPoints); err != nil {
			return fmt.Errorf("failed to index semantic vectors: %w", err)
		}
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
	if len(textPoints) > 0 {
		if err := s.repo.SaveTextVectors(ctx, textPoints); err != nil {
			log.Printf("Failed to index text vectors: %v", err)
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
				Message:      fmt.Sprintf("An exact match (100%% similarity) of your registered asset (%s) was verified by someone on the network.", cached.Sha256Hash),
			})
		}

		flags, _ := s.repo.GetFlagCount(ctx, cached.Sha256Hash)
		consensusCount, _ := s.repo.GetConsensusCount(ctx, cached.Sha256Hash)

		pubName, _, isPubVerified := s.resolvePublisher(ctx, cached.CreatorAddress)
		pubFlagCount, _ := s.repo.GetVerifiedPublisherFlagCount(ctx, cached.Sha256Hash)

		confidenceScore := 100.0
		if isPubVerified {
			confidenceScore = 100.0
		}
		if pubFlagCount > 0 {
			confidenceScore -= 50.0
			if confidenceScore < 0.0 {
				confidenceScore = 0.0
			}
		}

		confidenceTier := "High"
		if confidenceScore < 50.0 {
			confidenceTier = "Low"
		} else if confidenceScore < 80.0 {
			confidenceTier = "Medium"
		}

		matchDetail := MatchDetail{
			Sha256Hash:           cached.Sha256Hash,
			CreatorAddress:       cached.CreatorAddress,
			PHash:                cached.PHash,
			Similarity:           100.0,
			Timestamp:            cached.Timestamp,
			MediaType:            cached.MediaType,
			MatchType:            "exact",
			ConfidenceScore:      confidenceScore,
			ConfidenceTier:       confidenceTier,
			MediaIpfsUrl:         cached.MediaIpfsUrl,
			MediaS3Url:           cached.MediaS3Url,
			IpfsCid:              cached.IpfsCid,
			AiTool:               cached.AiTool,
			FlagCount:            flags,
			PublisherFlagCount:   pubFlagCount,
			ConsensusCount:       consensusCount,
			IsPublisherVerified:  isPubVerified,
			PublisherName:        pubName,
			Record:               cached,
		}

		return &VerificationResult{
			MatchFound:      true,
			ExactMatch:      true,
			Similarity:      100.0,
			Record:          cached,
			OnChainVerified: verified,
			OnChainTxHash:   txHash,
			Matches:         []MatchDetail{matchDetail},
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
			Message:      fmt.Sprintf("An exact match (100%% similarity) of your registered asset (%s) was verified by someone on the network.", record.Sha256Hash),
		})
	}

	flags, _ := s.repo.GetFlagCount(ctx, record.Sha256Hash)
	consensusCount, _ := s.repo.GetConsensusCount(ctx, record.Sha256Hash)

	pubName, _, isPubVerified := s.resolvePublisher(ctx, record.CreatorAddress)
	pubFlagCount, _ := s.repo.GetVerifiedPublisherFlagCount(ctx, record.Sha256Hash)

	confidenceScore := 100.0
	if isPubVerified {
		confidenceScore = 100.0
	}
	if pubFlagCount > 0 {
		confidenceScore -= 50.0
		if confidenceScore < 0.0 {
			confidenceScore = 0.0
		}
	}

	confidenceTier := "High"
	if confidenceScore < 50.0 {
		confidenceTier = "Low"
	} else if confidenceScore < 80.0 {
		confidenceTier = "Medium"
	}

	matchDetail := MatchDetail{
		Sha256Hash:           record.Sha256Hash,
		CreatorAddress:       record.CreatorAddress,
		PHash:                record.PHash,
		Similarity:           100.0,
		Timestamp:            record.Timestamp,
		MediaType:            record.MediaType,
		MatchType:            "exact",
		ConfidenceScore:      confidenceScore,
		ConfidenceTier:       confidenceTier,
		MediaIpfsUrl:         record.MediaIpfsUrl,
		MediaS3Url:           record.MediaS3Url,
		IpfsCid:              record.IpfsCid,
		AiTool:               record.AiTool,
		FlagCount:            flags,
		PublisherFlagCount:   pubFlagCount,
		ConsensusCount:       consensusCount,
		IsPublisherVerified:  isPubVerified,
		PublisherName:        pubName,
		Record:               record,
	}

	return &VerificationResult{
		MatchFound:      true,
		ExactMatch:      true,
		Similarity:      100.0,
		Record:          record,
		OnChainVerified: verified,
		OnChainTxHash:   txHash,
		Matches:         []MatchDetail{matchDetail},
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
	limit := uint32(5)
	matches, err := s.repo.SearchVectors(ctx, vec, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to search vectors: %w", err)
	}

	if len(matches) == 0 {
		return &VerificationResult{
			MatchFound: false,
		}, nil
	}

	threshold := 22.0
	var matchDetails []MatchDetail

	for _, match := range matches {
		distance := float64(match.GetScore())
		if distance > threshold {
			continue
		}

		payload := match.GetPayload()
		if payload == nil {
			continue
		}

		parentHashVal, ok := payload["parent_sha256"]
		if !ok {
			continue
		}
		parentHash := parentHashVal.GetStringValue()

		recordResult, err := s.VerifyExact(ctx, parentHash)
		if err != nil || !recordResult.MatchFound {
			continue
		}

		similarity := ((64.0 - distance) / 64.0) * 100.0
		verified, txHash := s.crossCheckBlockchain(ctx, recordResult.Record.Sha256Hash, recordResult.Record.IpfsCid)

		confidenceScore := similarity
		consensusCount, _ := s.repo.GetConsensusCount(ctx, recordResult.Record.Sha256Hash)
		if consensusCount > 1 {
			boost := float64(consensusCount-1) * 5.0
			if boost > 15.0 {
				boost = 15.0
			}
			confidenceScore += boost
			if confidenceScore > 100.0 {
				confidenceScore = 100.0
			}
		}

		pubName, _, isPubVerified := s.resolvePublisher(ctx, recordResult.Record.CreatorAddress)
		pubFlagCount, _ := s.repo.GetVerifiedPublisherFlagCount(ctx, recordResult.Record.Sha256Hash)

		if isPubVerified {
			confidenceScore = 100.0
		}
		if pubFlagCount > 0 {
			confidenceScore -= 50.0
			if confidenceScore < 0.0 {
				confidenceScore = 0.0
			}
		}

		confidenceTier := "High"
		if confidenceScore < 50.0 {
			confidenceTier = "Low"
		} else if confidenceScore < 80.0 {
			confidenceTier = "Medium"
		}

		flags, _ := s.repo.GetFlagCount(ctx, recordResult.Record.Sha256Hash)

		matchDetails = append(matchDetails, MatchDetail{
			Sha256Hash:           recordResult.Record.Sha256Hash,
			CreatorAddress:       recordResult.Record.CreatorAddress,
			PHash:                recordResult.Record.PHash,
			Similarity:           similarity,
			Timestamp:            recordResult.Record.Timestamp,
			MediaType:            recordResult.Record.MediaType,
			MatchType:            "similar",
			ConfidenceScore:      confidenceScore,
			ConfidenceTier:       confidenceTier,
			MediaIpfsUrl:         recordResult.Record.MediaIpfsUrl,
			MediaS3Url:           recordResult.Record.MediaS3Url,
			IpfsCid:              recordResult.Record.IpfsCid,
			AiTool:               recordResult.Record.AiTool,
			OnChainVerified:      verified,
			OnChainTxHash:        txHash,
			FlagCount:            flags,
			PublisherFlagCount:   pubFlagCount,
			ConsensusCount:       consensusCount,
			IsPublisherVerified:  isPubVerified,
			PublisherName:        pubName,
			Record:               recordResult.Record,
		})
	}

	if len(matchDetails) == 0 {
		return &VerificationResult{
			MatchFound: false,
		}, nil
	}

	sort.Slice(matchDetails, func(i, j int) bool {
		return matchDetails[i].Similarity > matchDetails[j].Similarity
	})

	topMatch := matchDetails[0]

	return &VerificationResult{
		MatchFound:      true,
		ExactMatch:      false,
		Similarity:      topMatch.Similarity,
		MediaType:       topMatch.MediaType,
		Record:          topMatch.Record,
		OnChainVerified: topMatch.Record.Sha256Hash != "",
		Matches:         matchDetails,
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
		
		flags, _ := s.repo.GetFlagCount(ctx, exactResult.Record.Sha256Hash)
		consensusCount, _ := s.repo.GetConsensusCount(ctx, exactResult.Record.Sha256Hash)

		pubName, _, isPubVerified := s.resolvePublisher(ctx, exactResult.Record.CreatorAddress)
		pubFlagCount, _ := s.repo.GetVerifiedPublisherFlagCount(ctx, exactResult.Record.Sha256Hash)

		confidenceScore := 100.0
		if isPubVerified {
			confidenceScore = 100.0
		}
		if pubFlagCount > 0 {
			confidenceScore -= 50.0
			if confidenceScore < 0.0 {
				confidenceScore = 0.0
			}
		}

		confidenceTier := "High"
		if confidenceScore < 50.0 {
			confidenceTier = "Low"
		} else if confidenceScore < 80.0 {
			confidenceTier = "Medium"
		}
		
		matchDetail := MatchDetail{
			Sha256Hash:           exactResult.Record.Sha256Hash,
			CreatorAddress:       exactResult.Record.CreatorAddress,
			PHash:                exactResult.Record.PHash,
			Similarity:           100.0,
			Timestamp:            exactResult.Record.Timestamp,
			MediaType:            exactResult.Record.MediaType,
			MatchType:            "exact",
			ConfidenceScore:      confidenceScore,
			ConfidenceTier:       confidenceTier,
			MediaIpfsUrl:         exactResult.Record.MediaIpfsUrl,
			MediaS3Url:           exactResult.Record.MediaS3Url,
			IpfsCid:              exactResult.Record.IpfsCid,
			AiTool:               exactResult.Record.AiTool,
			FlagCount:            flags,
			PublisherFlagCount:   pubFlagCount,
			ConsensusCount:       consensusCount,
			IsPublisherVerified:  isPubVerified,
			PublisherName:        pubName,
			Record:               exactResult.Record,
		}

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
			DebugVersion:            "v2",
			Matches:                 []MatchDetail{matchDetail},
		}, nil
	}

	pt := segmentPointType(mediaType)
	threshold := 22.0
	semanticThreshold := 0.85
	faceThreshold := 0.6
	audioThreshold := 0.999
	limit := uint32(5)

	vecs := make([][]float32, len(segments))
	semVecs := make([][]float32, len(segments))
	var faceVecs [][]float32

	for i, seg := range segments {
		vecs[i] = phashToVector(seg.PHash)
		semVecs[i] = seg.SemanticHash
		for _, fh := range seg.FaceHashes {
			faceVecs = append(faceVecs, fh)
		}
	}

	batchResults, err := s.repo.SearchVectorsBatch(ctx, vecs, limit, pt)
	if err != nil {
		log.Printf("SearchVectorsBatch failed: %v", err)
	}

	var semBatchResults [][]*pb.ScoredPoint
	if mediaType == "text" {
		semBatchResults, err = s.repo.SearchTextVectorsBatch(ctx, semVecs, limit, pt)
		if err != nil {
			log.Printf("SearchTextVectorsBatch failed: %v", err)
		}
	} else {
		semBatchResults, err = s.repo.SearchSemanticVectorsBatch(ctx, semVecs, limit, pt)
		if err != nil {
			log.Printf("SearchSemanticVectorsBatch failed: %v", err)
		}
	}

	var faceBatchResults [][]*pb.ScoredPoint
	if len(faceVecs) > 0 {
		faceBatchResults, err = s.repo.SearchFaceVectorsBatch(ctx, faceVecs, limit, pt)
		if err != nil {
			log.Printf("SearchFaceVectorsBatch failed: %v", err)
		}
	}

	var audioBatchResults [][]*pb.ScoredPoint
	if len(audioHash) > 0 {
		audioBatchResults, err = s.repo.SearchAudioVectorsBatch(ctx, [][]float32{audioHash}, limit, "video")
		if err != nil {
			log.Printf("SearchAudioVectorsBatch failed: %v", err)
		}
	}

	candidateParents := make(map[string]bool)

	visualMatchCounts := make(map[string]int)
	for _, results := range batchResults {
		for _, match := range results {
			if float64(match.GetScore()) > threshold {
				continue
			}
			payload := match.GetPayload()
			if payload == nil {
				continue
			}
			parentVal, ok := payload["parent_sha256"]
			if ok {
				pHashStr := parentVal.GetStringValue()
				visualMatchCounts[pHashStr]++
				candidateParents[pHashStr] = true
			}
		}
	}

	semMatchCounts := make(map[string]int)
	for _, results := range semBatchResults {
		for _, match := range results {
			if float64(match.GetScore()) < semanticThreshold {
				continue
			}
			payload := match.GetPayload()
			if payload == nil {
				continue
			}
			parentVal, ok := payload["parent_sha256"]
			if ok {
				pHashStr := parentVal.GetStringValue()
				semMatchCounts[pHashStr]++
				candidateParents[pHashStr] = true
			}
		}
	}

	faceMatchCounts := make(map[string]int)
	for _, results := range faceBatchResults {
		for _, match := range results {
			if float64(match.GetScore()) < faceThreshold {
				continue
			}
			payload := match.GetPayload()
			if payload == nil {
				continue
			}
			parentVal, ok := payload["parent_sha256"]
			if ok {
				pHashStr := parentVal.GetStringValue()
				faceMatchCounts[pHashStr]++
				candidateParents[pHashStr] = true
			}
		}
	}

	audioMatchCounts := make(map[string]int)
	for _, results := range audioBatchResults {
		for _, match := range results {
			if float64(match.GetScore()) < audioThreshold {
				continue
			}
			payload := match.GetPayload()
			if payload == nil {
				continue
			}
			parentVal, ok := payload["parent_sha256"]
			if ok {
				pHashStr := parentVal.GetStringValue()
				audioMatchCounts[pHashStr]++
				candidateParents[pHashStr] = true
			}
		}
	}

	var matchDetails []MatchDetail

	for parent := range candidateParents {
		visualCount := visualMatchCounts[parent]
		semCount := semMatchCounts[parent]
		faceCount := faceMatchCounts[parent]
		audioCount := audioMatchCounts[parent]

		coverageUploadedPct := 0.0
		if len(segments) > 0 {
			coverageUploadedPct = float64(visualCount) / float64(len(segments)) * 100.0
		}

		semCoverageUploadedPct := 0.0
		if len(segments) > 0 {
			semCoverageUploadedPct = float64(semCount) / float64(len(segments)) * 100.0
		}

		faceCoverageUploadedPct := 0.0
		if len(faceVecs) > 0 {
			faceCoverageUploadedPct = float64(faceCount) / float64(len(faceVecs)) * 100.0
		}

		isVisualMatch := coverageUploadedPct >= 5.0
		isFaceMatch := faceCoverageUploadedPct >= 10.0
		isSemMatch := semCoverageUploadedPct >= 10.0

		if !isVisualMatch && !isFaceMatch && !isSemMatch {
			continue
		}

		isDeepfake := false
		isAudioDeepfake := false
		finalCoveragePct := 0.0
		finalMatchedSegments := 0

		if isVisualMatch {
			finalCoveragePct = coverageUploadedPct
			finalMatchedSegments = visualCount

			if len(audioHash) > 0 && audioCount == 0 {
				isDeepfake = true
				isAudioDeepfake = true
			}
		} else if isFaceMatch {
			finalCoveragePct = faceCoverageUploadedPct
			finalMatchedSegments = faceCount
			isDeepfake = true
		} else if isSemMatch {
			finalCoveragePct = semCoverageUploadedPct
			finalMatchedSegments = semCount
			isDeepfake = true
		}

		parentResult, err := s.VerifyExact(ctx, parent)
		if err != nil || !parentResult.MatchFound {
			continue
		}

		totalRegistered, _ := s.repo.CountSegments(ctx, parent, pt)
		coverageRegisteredPct := 0.0
		if totalRegistered > 0 {
			coverageRegisteredPct = float64(finalMatchedSegments) / float64(totalRegistered) * 100.0
		}

		similarity := (finalCoveragePct + coverageRegisteredPct) / 2.0
		if isAudioDeepfake {
			similarity = similarity * 0.5
		}

		verified, txHash := s.crossCheckBlockchain(ctx, parentResult.Record.Sha256Hash, parentResult.Record.IpfsCid)

		temporalIntegrity := 0.0
		if finalMatchedSegments > 1 {
			var uploadedOffsets []float64
			var matchedOffsets []float64

			for i, results := range batchResults {
				for _, match := range results {
					if float64(match.GetScore()) > threshold {
						continue
					}
					payload := match.GetPayload()
					if payload == nil {
						continue
					}
					parentVal, ok := payload["parent_sha256"]
					if ok && parentVal.GetStringValue() == parent {
						offsetVal, okOffset := payload["timestamp_offset"]
						if okOffset {
							uploadedOffsets = append(uploadedOffsets, float64(i))
							matchedOffsets = append(matchedOffsets, float64(offsetVal.GetIntegerValue()))
							break
						}
					}
				}
			}

			if len(uploadedOffsets) > 1 && len(matchedOffsets) > 1 {
				shift := matchedOffsets[0] - uploadedOffsets[0]
				normalizedDist := 0.0
				for i := range matchedOffsets {
					expected := uploadedOffsets[i] + shift
					normalizedDist += math.Abs(matchedOffsets[i] - expected)
				}

				maxError := float64(len(matchedOffsets)) * float64(len(matchedOffsets))
				integrity := 100.0 * (1.0 - (normalizedDist / maxError))
				if integrity < 0 {
					integrity = 0
				}
				if integrity > 100 {
					integrity = 100
				}
				temporalIntegrity = integrity
			}
		}

		confidenceScore := similarity
		if temporalIntegrity > 0 {
			confidenceScore = (similarity * 0.7) + (temporalIntegrity * 0.3)
		}
		if isDeepfake {
			confidenceScore = confidenceScore * 0.5
		}

		consensusCount, _ := s.repo.GetConsensusCount(ctx, parentResult.Record.Sha256Hash)
		if consensusCount > 1 {
			boost := float64(consensusCount-1) * 5.0
			if boost > 15.0 {
				boost = 15.0
			}
			confidenceScore += boost
			if confidenceScore > 100.0 {
				confidenceScore = 100.0
			}
		}

		pubName, _, isPubVerified := s.resolvePublisher(ctx, parentResult.Record.CreatorAddress)
		pubFlagCount, _ := s.repo.GetVerifiedPublisherFlagCount(ctx, parentResult.Record.Sha256Hash)

		if isPubVerified {
			confidenceScore = 100.0
		}
		if pubFlagCount > 0 {
			confidenceScore -= 50.0
			if confidenceScore < 0.0 {
				confidenceScore = 0.0
			}
		}

		confidenceTier := "High"
		if confidenceScore < 50.0 {
			confidenceTier = "Low"
		} else if confidenceScore < 80.0 {
			confidenceTier = "Medium"
		}

		matchType := "similar"
		if isDeepfake {
			matchType = "deepfake"
		}

		flags, _ := s.repo.GetFlagCount(ctx, parentResult.Record.Sha256Hash)

		matchDetails = append(matchDetails, MatchDetail{
			Sha256Hash:           parentResult.Record.Sha256Hash,
			CreatorAddress:       parentResult.Record.CreatorAddress,
			PHash:                parentResult.Record.PHash,
			Similarity:           similarity,
			Timestamp:            parentResult.Record.Timestamp,
			MediaType:            parentResult.Record.MediaType,
			MatchType:            matchType,
			IsDeepfake:           isDeepfake,
			IsAudioDeepfake:      isAudioDeepfake,
			TemporalIntegrity:    temporalIntegrity,
			ConfidenceScore:      confidenceScore,
			ConfidenceTier:       confidenceTier,
			MediaIpfsUrl:         parentResult.Record.MediaIpfsUrl,
			MediaS3Url:           parentResult.Record.MediaS3Url,
			IpfsCid:              parentResult.Record.IpfsCid,
			AiTool:               parentResult.Record.AiTool,
			OnChainVerified:      verified,
			OnChainTxHash:        txHash,
			MatchedSegments:      finalMatchedSegments,
			FlagCount:            flags,
			PublisherFlagCount:   pubFlagCount,
			ConsensusCount:       consensusCount,
			IsPublisherVerified:  isPubVerified,
			PublisherName:        pubName,
			Record:               parentResult.Record,
		})
	}

	if len(matchDetails) == 0 {
		return &SegmentVerificationResult{MatchFound: false}, nil
	}

	sort.Slice(matchDetails, func(i, j int) bool {
		return matchDetails[i].Similarity > matchDetails[j].Similarity
	})

	topMatch := matchDetails[0]

	// Send webhook alert for top plagiarism concern if registered Creator doesn't match uploader
	if topMatch.Similarity >= 80.0 && topMatch.CreatorAddress != sha256 { // sha256 is the query uploader in this context
		if topMatch.Record.WebhookUrl != "" {
			msg := fmt.Sprintf("A matching segment of your registered asset (%s) was verified by someone on the network. The uploaded content is %.1f%% similar to your original work.", topMatch.Record.Sha256Hash, topMatch.Similarity)
			if topMatch.IsAudioDeepfake {
				msg = fmt.Sprintf("CRITICAL ALERT: An Audio Deepfake (Voice Cloning) of your registered video (%s) was detected during a verification check! The visual content matches, but the audio track has been maliciously manipulated.", topMatch.Record.Sha256Hash)
			} else if topMatch.IsDeepfake {
				msg = fmt.Sprintf("CRITICAL ALERT: A Deepfake or AI-altered version of your registered asset (%s) was detected during a verification check!", topMatch.Record.Sha256Hash)
			}
			s.dispatcher.Dispatch(topMatch.Record.WebhookUrl, webhook.Payload{
				EventType:    webhook.EventDerivativeDetected,
				OriginalHash: topMatch.Record.Sha256Hash,
				Similarity:   topMatch.Similarity,
				Timestamp:    time.Now().UTC().Format(time.RFC3339),
				Message:      msg,
			})
		}
	}

	topMatchTotalRegistered, _ := s.repo.CountSegments(ctx, topMatch.Sha256Hash, pt)

	result := &SegmentVerificationResult{
		MatchFound:              true,
		ExactMatch:              false,
		Similarity:              topMatch.Similarity,
		MatchedSegments:         topMatch.MatchedSegments,
		TotalSegmentsUploaded:   len(segments),
		TotalSegmentsRegistered: topMatchTotalRegistered,
		CoverageUploadedPct:     topMatch.Similarity,
		CoverageRegisteredPct:   topMatch.Similarity,
		Record:                  topMatch.Record,
		OnChainVerified:         topMatch.OnChainVerified,
		OnChainTxHash:           topMatch.OnChainTxHash,
		IsDeepfake:              topMatch.IsDeepfake,
		IsAudioDeepfake:         topMatch.IsAudioDeepfake,
		TemporalIntegrity:       topMatch.TemporalIntegrity,
		DebugVersion:            "v2",
		Matches:                 matchDetails,
	}

	_ = s.repo.SaveSegmentCache(ctx, cacheKey, result)

	return result, nil
}

func segmentPointType(mediaType string) string {
	if mediaType == "video" {
		return "keyframe"
	} else if mediaType == "document" {
		return "page"
	} else if mediaType == "text" {
		return "text"
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

func (s *service) buildSemanticPoint(sha256, creator string, semanticHash []float32, offset uint64, mediaType, pointType string, caption string) *pb.PointStruct {
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

	if caption != "" {
		payload["caption"] = pb.NewValueString(caption)
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
		if (phash & (1 << (63 - i))) != 0 {
			vec[i] = 1.0
		} else {
			vec[i] = -1.0
		}
	}
	return vec
}

// dtwDistance calculates the Dynamic Time Warping distance between two sequences
func dtwDistance(a, b []float64) float64 {
	n := len(a)
	m := len(b)
	dtw := make([][]float64, n+1)
	for i := range dtw {
		dtw[i] = make([]float64, m+1)
		for j := range dtw[i] {
			dtw[i][j] = math.Inf(1)
		}
	}
	dtw[0][0] = 0

	for i := 1; i <= n; i++ {
		for j := 1; j <= m; j++ {
			cost := math.Abs(a[i-1] - b[j-1])
			dtw[i][j] = cost + math.Min(dtw[i-1][j], math.Min(dtw[i][j-1], dtw[i-1][j-1]))
		}
	}
	return dtw[n][m]
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

	hasher := sha256.New()
	hasher.Write(data)
	contentHash := hex.EncodeToString(hasher.Sum(nil))

	ext := ""
	for i := len(filename) - 1; i >= 0; i-- {
		if filename[i] == '.' {
			ext = filename[i:]
			break
		}
	}
	uniqueFilename := contentHash + ext

	ipfsHash, err := s.pinFileToIPFS(ctx, bytes.NewReader(data), uniqueFilename)
	var ipfsUrl string
	if err != nil {
		mockHash := "QmMock" + contentHash[:30]
		ipfsUrl = fmt.Sprintf("https://gateway.pinata.cloud/ipfs/%s", mockHash)
		log.Printf("WARNING: Pinata IPFS upload failed (%v). Falling back to mock IPFS URL for testing: %s", err, ipfsUrl)
	} else {
		ipfsUrl = fmt.Sprintf("https://gateway.pinata.cloud/ipfs/%s", ipfsHash)
	}

	s3Url, err := s.storage.UploadFile(ctx, bytes.NewReader(data), uniqueFilename, contentType)
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

func (s *service) FlagContent(ctx context.Context, hash, reporter, reason string, timestamp int64) error {
	return s.repo.FlagContent(ctx, hash, reporter, reason, timestamp)
}

func (s *service) resolvePublisher(ctx context.Context, address string) (string, string, bool) {
	// 1. Try On-Chain Smart Contract call first (Arbitrum L2 whitelist)
	if s.onchainVerifier != nil {
		org, isVerified, err := s.onchainVerifier.IsVerifiedPublisher(ctx, address)
		if err == nil && isVerified {
			return org, "Arbitrum L2 Contract", true
		}
	}

	// 2. Fallback to Local PostgreSQL database cache
	org, domain, isVerified, err := s.repo.GetVerifiedPublisher(ctx, address)
	if err == nil && isVerified {
		return org, domain, true
	}

	return "", "", false
}

func (s *service) VerifyPublisherDomain(ctx context.Context, domain, address string) error {
	domainClean := strings.TrimSpace(strings.ToLower(domain))
	if domainClean == "" {
		return fmt.Errorf("domain cannot be empty")
	}

	url := fmt.Sprintf("https://%s/.well-known/veritrace.json", domainClean)
	
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch verification file from %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("domain returned HTTP status %d", resp.StatusCode)
	}

	var payload struct {
		OrganizationName string `json:"organization_name"`
		Domain           string `json:"domain"`
		CreatorAddress   string `json:"creator_address"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return fmt.Errorf("failed to decode verification JSON: %w", err)
	}

	if strings.ToLower(payload.CreatorAddress) != strings.ToLower(address) {
		return fmt.Errorf("address in verification file (%s) does not match requested wallet (%s)", payload.CreatorAddress, address)
	}

	if strings.ToLower(payload.Domain) != domainClean {
		return fmt.Errorf("domain in verification file (%s) does not match requested domain (%s)", payload.Domain, domainClean)
	}

	verifiedAt := time.Now().Unix()
	err = s.repo.SaveVerifiedPublisher(ctx, address, payload.OrganizationName, domainClean, verifiedAt)
	if err != nil {
		return fmt.Errorf("failed to save publisher to database: %w", err)
	}

	return nil
}

func (s *service) ListVerifiedPublishers(ctx context.Context) ([]database.VerifiedPublisher, error) {
	return s.repo.ListVerifiedPublishers(ctx)
}
