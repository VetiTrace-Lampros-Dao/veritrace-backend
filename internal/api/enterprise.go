package api

import (
	"database/sql"
	"fmt"
	"math/big"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type EnterpriseHandler struct {
	db *sql.DB
}

func NewEnterpriseHandler(db *sql.DB) *EnterpriseHandler {
	return &EnterpriseHandler{db: db}
}

func (h *EnterpriseHandler) QueryDataset(c *gin.Context) {
	mediaType := c.Query("type")
	quantityStr := c.Query("quantity")

	if mediaType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "media type is required"})
		return
	}

	quantity, err := strconv.Atoi(quantityStr)
	if err != nil || quantity <= 0 {
		quantity = 100 // default
	}

	// Fetch up to 'quantity' items where AllowAiTraining is true
	query := `
		SELECT sha256_hash, creator_address
		FROM content_records
		WHERE media_type = $1 AND allow_ai_training = true
		LIMIT $2;
	`
	rows, err := h.db.QueryContext(c.Request.Context(), query, mediaType, quantity)
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
		c.JSON(http.StatusNotFound, gin.H{"error": "no data found for this type"})
		return
	}

	// Math logic: $1 USDC per item. (We use $1 for easy math, but 0.95 after 5% fee)
	// USDC has 6 decimals.
	// Total price = totalFound * 1 USDC
	totalUSDC := totalFound * 1000000 // 1 USDC = 1,000,000 units
	fee := float64(totalUSDC) * 0.05
	distributable := float64(totalUSDC) - fee

	perItemAmount := distributable / float64(totalFound)

	var creators []string
	var amounts []string // String to prevent precision loss in JS

	for creator, count := range creatorCounts {
		creators = append(creators, creator)
		amount := new(big.Float).SetFloat64(perItemAmount * float64(count))
		amountInt, _ := amount.Int(nil)
		amounts = append(amounts, amountInt.String())
	}

	c.JSON(http.StatusOK, gin.H{
		"total_items": totalFound,
		"total_usdc":  totalUSDC,
		"platform_fee": int64(fee),
		"creators":    creators,
		"amounts":     amounts,
		"hashes":      hashes, // usually would be kept hidden until payment, but returning for demo purposes
		"message":     fmt.Sprintf("Found %d items. Submit payment via smart contract to unlock high-res S3 URLs.", totalFound),
	})
}
