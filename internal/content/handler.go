package content

import (
	"net/http"
	"strconv"

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


