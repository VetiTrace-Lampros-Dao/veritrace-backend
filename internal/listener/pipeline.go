package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/content"
	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/database"
)

type KeyframePayload struct {
	Offset uint64 `json:"offset"`
	PHash  uint64 `json:"phash"`
}

type MetadataJSON struct {
	SHA256              string            `json:"sha256"`
	RepresentativePHash uint64            `json:"representative_phash"`
	MediaType           string            `json:"media_type"`
	MediaIpfsUrl        string            `json:"media_ipfs_url"`
	MediaS3Url          string            `json:"media_s3_url"`
	AllowAiTraining     bool              `json:"allow_ai_training"`
	WebhookUrl          string            `json:"webhook_url"`
	ParentSha256        string            `json:"parent_sha256"`
	Keyframes           []KeyframePayload `json:"keyframes"`
}

type Pipeline struct {
	cfg            *config.Config
	contentService content.Service
	listener       *EVMListener
}

func NewPipeline(cfg *config.Config, contentService content.Service, listener *EVMListener) *Pipeline {
	return &Pipeline{
		cfg:            cfg,
		contentService: contentService,
		listener:       listener,
	}
}

func (p *Pipeline) Start(ctx context.Context, numWorkers int) {
	events := p.listener.Events()
	for i := 0; i < numWorkers; i++ {
		go p.worker(ctx, events)
	}
}

func (p *Pipeline) worker(ctx context.Context, events <-chan EventPayload) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if err := p.processEvent(ctx, event); err != nil {
				log.Printf("Pipeline: Error processing event for SHA256 %s: %v", event.Sha256Hash, err)
			}
		}
	}
}

func (p *Pipeline) processEvent(ctx context.Context, event EventPayload) error {
	record := database.ContentRecord{
		Sha256Hash:     event.Sha256Hash,
		CreatorAddress: event.CreatorAddress,
		PHash:          event.PHash,
		Timestamp:      event.Timestamp,
		IpfsCid:        event.IpfsCid,
		AiTool:         event.AiTool,
	}

	mediaType := "image"
	var keyframes []content.KeyframePayload

	meta, err := p.fetchMetadata(ctx, event.IpfsCid)
	if err == nil && meta != nil {
		if meta.MediaType != "" {
			mediaType = meta.MediaType
		} else if len(meta.Keyframes) > 0 {
			mediaType = "video"
		}
		for _, kf := range meta.Keyframes {
			keyframes = append(keyframes, content.KeyframePayload{
				Offset: kf.Offset,
				PHash:  kf.PHash,
			})
		}
		record.MediaIpfsUrl = meta.MediaIpfsUrl
		record.MediaS3Url = meta.MediaS3Url
		record.AllowAiTraining = meta.AllowAiTraining
		record.WebhookUrl = meta.WebhookUrl
		record.ParentSha256 = meta.ParentSha256
	} else if err != nil {
		log.Printf("Pipeline warning: failed to fetch metadata for IPFS CID %s: %v", event.IpfsCid, err)
	}

	record.MediaType = mediaType

	if err := p.contentService.Register(ctx, record, keyframes, mediaType); err != nil {
		return fmt.Errorf("failed to register content: %w", err)
	}

	log.Printf("Pipeline successfully synced SHA256 %s", event.Sha256Hash)
	return nil
}

func (p *Pipeline) fetchMetadata(ctx context.Context, ipfsCid string) (*MetadataJSON, error) {
	if ipfsCid == "" {
		return nil, fmt.Errorf("empty IPFS CID")
	}

	url := fmt.Sprintf("https://ipfs.io/ipfs/%s", ipfsCid)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	client := http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP status %d", resp.StatusCode)
	}

	var meta MetadataJSON
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

