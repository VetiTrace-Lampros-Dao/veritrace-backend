package api

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/internal/vector"
	"github.com/gin-gonic/gin"
	pb "github.com/qdrant/go-client/qdrant"
)

type EnterpriseHandler struct {
	db     *sql.DB
	qdrant *vector.QdrantClient
}

func NewEnterpriseHandler(db *sql.DB, qdrant *vector.QdrantClient) *EnterpriseHandler {
	return &EnterpriseHandler{db: db, qdrant: qdrant}
}

func (h *EnterpriseHandler) QueryDataset(c *gin.Context) {
	mediaType := c.Query("type")
	quantityStr := c.Query("quantity")
	searchQuery := c.Query("query")

	if mediaType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "media type is required"})
		return
	}

	quantity, err := strconv.Atoi(quantityStr)
	if err != nil || quantity <= 0 {
		quantity = 100 // default
	}

	var semanticHashes []string

	// If a semantic search query is provided, fetch embedding and search Qdrant
	if searchQuery != "" && h.qdrant != nil {
		payload := map[string]string{"text": searchQuery}
		payloadBytes, _ := json.Marshal(payload)

		// Note: For production, this URL should be configurable via env
		aiURL := "http://host.docker.internal:8082/api/v1/embed_text_clip"
		req, _ := http.NewRequest("POST", aiURL, bytes.NewBuffer(payloadBytes))
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)

		if err == nil && resp.StatusCode == 200 {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)

			var aiRes struct {
				SemanticHash []float32 `json:"semantic_hash"`
			}
			if err := json.Unmarshal(body, &aiRes); err == nil && len(aiRes.SemanticHash) > 0 {
				// Query Qdrant with the embedding
				limit := uint64(quantity * 2)
				scoreThreshold := float32(0.22) // Require a minimum similarity score (Cosine similarity)
				qResp, err := h.qdrant.Points.Search(c.Request.Context(), &pb.SearchPoints{
					CollectionName: "veritrace_semantics",
					Vector:         aiRes.SemanticHash,
					Limit:          limit,
					ScoreThreshold: &scoreThreshold,
					WithPayload: &pb.WithPayloadSelector{
						SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true},
					},
				})

				if err == nil && qResp != nil {
					for _, point := range qResp.GetResult() {
						if payload, ok := point.Payload["parent_sha256"]; ok {
							parentHash := payload.GetStringValue()
							if parentHash != "" {
								semanticHashes = append(semanticHashes, parentHash)
							}
						}
					}
				}
			}
		}
	}

	// Fetch items from PostgreSQL
	var query string
	var args []interface{}

	if len(semanticHashes) > 0 {
		// Use semantic hashes to filter
		var placeholders string
		for i, hash := range semanticHashes {
			args = append(args, hash)
			if i > 0 {
				placeholders += ", "
			}
			placeholders += fmt.Sprintf("$%d", i+1)
		}

		query = fmt.Sprintf(`
			SELECT sha256_hash, creator_address
			FROM content_records
			WHERE sha256_hash IN (%s) AND media_type = $%d AND allow_ai_training = true
			LIMIT $%d;
		`, placeholders, len(semanticHashes)+1, len(semanticHashes)+2)

		args = append(args, mediaType, quantity)
	} else {
		// Fallback to standard query if no search or no results
		query = `
			SELECT sha256_hash, creator_address
			FROM content_records
			WHERE media_type = $1 AND allow_ai_training = true
			LIMIT $2;
		`
		args = []interface{}{mediaType, quantity}
	}

	rows, err := h.db.QueryContext(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query database"})
		return
	}
	defer rows.Close()

	creatorCounts := make(map[string]int)
	totalFound := 0

	var hashes []string

	for rows.Next() {
		var hash, creator string
		if err := rows.Scan(&hash, &creator); err != nil {
			continue
		}
		creatorCounts[creator]++
		hashes = append(hashes, hash)
		totalFound++
	}

	if totalFound == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "no data found for this query"})
		return
	}

	// Math logic: $1 USDC per item. (We use $1 for easy math, but 0.95 after 5% fee)
	totalUSDC := totalFound * 1000000 // 1 USDC = 1,000,000 units
	fee := float64(totalUSDC) * 0.05
	distributable := float64(totalUSDC) - fee

	perItemAmount := distributable / float64(totalFound)

	var creators []string
	var amounts []string

	for creator, count := range creatorCounts {
		creators = append(creators, creator)
		amount := new(big.Float).SetFloat64(perItemAmount * float64(count))
		amountInt, _ := amount.Int(nil)
		amounts = append(amounts, amountInt.String())
	}

	// Re-fetch vectors for the selected hashes to provide to the user
	semanticEmbeddings := make(map[string][]float32)
	captions := make(map[string]string)

	if len(hashes) > 0 && h.qdrant != nil {
		var shouldConditions []*pb.Condition
		for _, hash := range hashes {
			shouldConditions = append(shouldConditions, &pb.Condition{
				ConditionOneOf: &pb.Condition_Field{
					Field: &pb.FieldCondition{
						Key: "parent_sha256",
						Match: &pb.Match{
							MatchValue: &pb.Match_Keyword{
								Keyword: hash,
							},
						},
					},
				},
			})
		}

		limit := uint32(len(hashes) * 2)
		resp, err := h.qdrant.Points.Scroll(c.Request.Context(), &pb.ScrollPoints{
			CollectionName: "veritrace_semantics",
			Limit:          &limit,
			WithPayload: &pb.WithPayloadSelector{
				SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true},
			},
			WithVectors: &pb.WithVectorsSelector{
				SelectorOptions: &pb.WithVectorsSelector_Enable{Enable: true},
			},
			Filter: &pb.Filter{
				Should: shouldConditions,
			},
		})

		if err == nil && resp != nil {
			for _, point := range resp.GetResult() {
				if payload, ok := point.Payload["parent_sha256"]; ok {
					parentHash := payload.GetStringValue()
					if parentHash != "" && point.Vectors != nil {
						if vec := point.Vectors.GetVector(); vec != nil {
							semanticEmbeddings[parentHash] = vec.Data
						}
					}
					if capPayload, ok := point.Payload["caption"]; ok {
						captions[parentHash] = capPayload.GetStringValue()
					}
				}
			}
		}
	}

	message := fmt.Sprintf("Found %d items.", totalFound)
	if searchQuery != "" {
		message = fmt.Sprintf("Found %d items matching '%s'.", totalFound, searchQuery)
	}
	message += " Submit payment via smart contract to unlock high-res S3 URLs."

	c.JSON(http.StatusOK, gin.H{
		"total_items":         totalFound,
		"total_usdc":          totalUSDC,
		"platform_fee":        int64(fee),
		"creators":            creators,
		"amounts":             amounts,
		"hashes":              hashes,
		"semantic_embeddings": semanticEmbeddings,
		"captions":            captions,
		"message":             message,
	})
}

type UnlockRequest struct {
	TxHash string   `json:"txHash" binding:"required"`
	Hashes []string `json:"hashes" binding:"required"`
}

func (h *EnterpriseHandler) UnlockDataset(c *gin.Context) {
	var req UnlockRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	// 1. Check if txHash has already been used
	var exists bool
	err := h.db.QueryRowContext(c.Request.Context(), "SELECT EXISTS(SELECT 1 FROM used_transactions WHERE tx_hash = $1)", req.TxHash).Scan(&exists)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database error checking transaction"})
		return
	}
	if exists {
		c.JSON(http.StatusForbidden, gin.H{"error": "Transaction hash already used to unlock a dataset"})
		return
	}

	// 2. Mark txHash as used
	_, err = h.db.ExecContext(c.Request.Context(), "INSERT INTO used_transactions (tx_hash) VALUES ($1)", req.TxHash)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record transaction"})
		return
	}

	// 3. Fetch S3 URLs for the provided hashes
	if len(req.Hashes) == 0 {
		c.JSON(http.StatusOK, gin.H{"links": []string{}})
		return
	}

	// Build query for multiple hashes
	query := "SELECT media_s3_url FROM content_records WHERE sha256_hash = ANY($1)"

	// Convert Go slice to a PostgreSQL array string or use pq.Array (but pq is imported in database, not here)
	// Alternatively, iterate over hashes and build IN clause (or use pq.Array if imported).
	// For simplicity in gin handlers, we can build the query with numbered parameters:
	var args []interface{}
	var placeholders string
	for i, hash := range req.Hashes {
		args = append(args, hash)
		if i > 0 {
			placeholders += ", "
		}
		placeholders += fmt.Sprintf("$%d", i+1)
	}

	query = fmt.Sprintf("SELECT sha256_hash, media_s3_url FROM content_records WHERE sha256_hash IN (%s)", placeholders)
	rows, err := h.db.QueryContext(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch S3 links"})
		return
	}
	defer rows.Close()

	type UnlockResult struct {
		Hash  string `json:"hash"`
		S3Url string `json:"s3_url"`
	}

	var results []UnlockResult
	for rows.Next() {
		var hash, s3Url string
		if err := rows.Scan(&hash, &s3Url); err == nil {
			results = append(results, UnlockResult{Hash: hash, S3Url: s3Url})
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Dataset successfully unlocked!",
		"links":   results,
	})
}
