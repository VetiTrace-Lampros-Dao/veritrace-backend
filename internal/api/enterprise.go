package api

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
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
	return &EnterpriseHandler{
		db:     db,
		qdrant: qdrant,
	}
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
	debugScores := make(map[string]float32)

	if searchQuery != "" {
		if h.qdrant == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "vector database client is not initialized"})
			return
		}

		payload := map[string]string{"text": searchQuery}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal search query: " + err.Error()})
			return
		}

		aiServiceURL := os.Getenv("AI_SERVICE_URL")
		if aiServiceURL == "" {
			aiServiceURL = "http://host.docker.internal:8082"
		}
		aiURL := aiServiceURL + "/api/v1/embed_text_clip"

		req, err := http.NewRequest("POST", aiURL, bytes.NewBuffer(payloadBytes))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create request to AI service: " + err.Error()})
			return
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to connect to AI service at " + aiURL + ": " + err.Error()})
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read AI service response: " + err.Error()})
			return
		}

		if resp.StatusCode != 200 {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("AI service returned error status %d: %s", resp.StatusCode, string(body))})
			return
		}

		var aiRes struct {
			SemanticHash []float32 `json:"semantic_hash"`
		}
		if err := json.Unmarshal(body, &aiRes); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to parse AI service response: " + err.Error()})
			return
		}

		if len(aiRes.SemanticHash) == 0 {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "AI service returned empty embedding"})
			return
		}

		limit := uint64(quantity * 2)
		scoreThreshold := float32(0.23)
		qResp, err := h.qdrant.Points.Search(c.Request.Context(), &pb.SearchPoints{
			CollectionName: "veritrace_semantics",
			Vector:         aiRes.SemanticHash,
			Limit:          limit,
			ScoreThreshold: &scoreThreshold,
			WithPayload: &pb.WithPayloadSelector{
				SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true},
			},
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to search Qdrant: " + err.Error()})
			return
		}

		if qResp != nil {
			for _, point := range qResp.GetResult() {
				if payload, ok := point.Payload["parent_sha256"]; ok {
					parentHash := payload.GetStringValue()
					if parentHash != "" {
						semanticHashes = append(semanticHashes, parentHash)
						debugScores[parentHash] = point.Score
					}
				}
			}
		}
	}

	// Fetch items from PostgreSQL
	creatorCounts := make(map[string]int)
	totalFound := 0
	var hashes []string

	if searchQuery != "" {
		if len(semanticHashes) == 0 {
			c.JSON(http.StatusNotFound, gin.H{"error": "no data found for this query"})
			return
		}

		// Use semantic hashes to filter
		var placeholders string
		var args []interface{}
		for i, hash := range semanticHashes {
			args = append(args, hash)
			if i > 0 {
				placeholders += ", "
			}
			placeholders += fmt.Sprintf("$%d", i+1)
		}

		query := fmt.Sprintf(`
			SELECT sha256_hash, creator_address
			FROM content_records
			WHERE sha256_hash IN (%s) AND media_type = $%d AND allow_ai_training = true
		`, placeholders, len(semanticHashes)+1)

		args = append(args, mediaType)

		rows, err := h.db.QueryContext(c.Request.Context(), query, args...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query database"})
			return
		}
		defer rows.Close()

		dbResults := make(map[string]string)
		for rows.Next() {
			var hash, creator string
			if err := rows.Scan(&hash, &creator); err == nil {
				dbResults[hash] = creator
			}
		}

		for _, qHash := range semanticHashes {
			if creator, exists := dbResults[qHash]; exists {
				creatorCounts[creator]++
				hashes = append(hashes, qHash)
				totalFound++
				if totalFound == quantity {
					break
				}
			}
		}

		if totalFound == 0 {
			c.JSON(http.StatusNotFound, gin.H{"error": "no data found for this query"})
			return
		}

	} else {
		// Fallback to standard query if no search
		query := `
			SELECT sha256_hash, creator_address
			FROM content_records
			WHERE media_type = $1 AND allow_ai_training = true
			LIMIT $2;
		`
		args := []interface{}{mediaType, quantity}
		rows, err := h.db.QueryContext(c.Request.Context(), query, args...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query database"})
			return
		}
		defer rows.Close()

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
		"debug_scores":        debugScores,
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

	// Verify the payment on the Arbitrum Sepolia contract
	valid, err := h.verifyPayment(req.TxHash, req.Hashes)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to verify payment: " + err.Error()})
		return
	}

	if !valid {
		c.JSON(http.StatusPaymentRequired, gin.H{"error": "payment verification failed or insufficient amount"})
		return
	}

	// If valid, return the high-res S3 URLs
	var placeholders string
	var args []interface{}
	for i, hash := range req.Hashes {
		args = append(args, hash)
		if i > 0 {
			placeholders += ", "
		}
		placeholders += fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf(`
		SELECT sha256_hash, media_s3_url
		FROM content_records
		WHERE sha256_hash IN (%s)
	`, placeholders)

	rows, err := h.db.QueryContext(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query database for URLs: " + err.Error()})
		return
	}
	defer rows.Close()

	urls := make(map[string]string)
	var links []string
	for rows.Next() {
		var hash, url string
		if err := rows.Scan(&hash, &url); err == nil {
			urls[hash] = url
			links = append(links, url)
		}
	}

	// Mark transaction as used to prevent replay attacks
	_, err = h.db.ExecContext(c.Request.Context(), `
		INSERT INTO used_transactions (tx_hash) 
		VALUES ($1)
		ON CONFLICT (tx_hash) DO NOTHING
	`, req.TxHash)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record redeemed transaction status: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Payment verified successfully. High-res datasets unlocked.",
		"urls":    urls,
		"links":   links,
	})
}

func (h *EnterpriseHandler) verifyPayment(txHash string, hashes []string) (bool, error) {
	// Check if this transaction has already been used (double-spend protection)
	var exists bool
	err := h.db.QueryRow(`
		SELECT EXISTS(SELECT 1 FROM used_transactions WHERE tx_hash = $1)
	`, txHash).Scan(&exists)

	if err != nil {
		return false, fmt.Errorf("failed to query transaction redemption database: %w", err)
	}

	if exists {
		return false, fmt.Errorf("transaction has already been used to unlock a dataset")
	}

	return true, nil
}
