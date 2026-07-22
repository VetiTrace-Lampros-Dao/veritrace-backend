package content

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	service Service
}

func NewHandler(service Service) *Handler {
	return &Handler{
		service: service,
	}
}

func (h *Handler) VerifyExact(c *gin.Context) {
	hash := c.Query("hash")
	if hash == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing hash parameter"})
		return
	}

	result, err := h.service.VerifyExact(c.Request.Context(), hash)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) ExportCertificate(c *gin.Context) {
	hash := c.Query("hash")
	if hash == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing hash parameter"})
		return
	}

	cert, err := h.service.GenerateCertificate(c.Request.Context(), hash)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cert)
}

func (h *Handler) VerifyFuzzy(c *gin.Context) {
	phashStr := c.Query("phash")
	if phashStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing phash parameter"})
		return
	}

	phash, err := strconv.ParseUint(phashStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid phash format"})
		return
	}

	result, err := h.service.VerifyFuzzy(c.Request.Context(), phash)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) PinToIPFS(c *gin.Context) {
	var payload interface{}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json payload: " + err.Error()})
		return
	}

	ipfsCID, err := h.service.PinToIPFS(c.Request.Context(), payload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ipfs_cid": ipfsCID})
}

func (h *Handler) PinFile(c *gin.Context) {
	header, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing file parameter: " + err.Error()})
		return
	}

	file, err := header.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to open uploaded file: " + err.Error()})
		return
	}
	defer file.Close()

	ipfsUrl, s3Url, err := h.service.PinFile(c.Request.Context(), file, header.Filename, header.Header.Get("Content-Type"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"media_ipfs_url": ipfsUrl,
		"media_s3_url":   s3Url,
	})
}
func (h *Handler) VerifySegments(c *gin.Context) {
	var req struct {
		SHA256    string            `json:"sha256"`
		MediaType string            `json:"media_type"`
		AudioHash []float32         `json:"audio_hashes,omitempty"`
		Segments  []KeyframePayload `json:"segments"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	if req.SHA256 == "" || req.MediaType == "" || len(req.Segments) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sha256, media_type, and segments are required"})
		return
	}

	result, err := h.service.VerifySegments(c.Request.Context(), req.SHA256, req.Segments, req.MediaType, req.AudioHash)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) GetLineage(c *gin.Context) {
	hash := c.Param("hash")
	if hash == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing hash"})
		return
	}

	chain, err := h.service.GetLineage(c.Request.Context(), hash)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if len(chain) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "no lineage found for this hash"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"lineage": chain, "depth": len(chain)})
}

func (h *Handler) FlagContent(c *gin.Context) {
	var req struct {
		SHA256   string `json:"sha256" binding:"required"`
		Reporter string `json:"reporter" binding:"required"`
		Reason   string `json:"reason" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}

	timestamp := time.Now().Unix()
	err := h.service.FlagContent(c.Request.Context(), req.SHA256, req.Reporter, req.Reason, timestamp)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record flag: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "content flagged successfully"})
}
