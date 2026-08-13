package config

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port             string
	DBHost           string
	DBPort           string
	DBUser           string
	DBPassword       string
	DBName           string
	DBSslMode        string
	RedisHost        string
	RedisPort        string
	RedisPassword    string
	RedisDB          int
	ContractAddress  string
	QdrantHost       string
	QdrantPort       string
	ArbitrumWS       string
	PinataJWT        string
	PinataGateway    string
	S3Endpoint       string
	S3PublicEndpoint string
	S3AccessKey      string
	S3SecretKey      string
	S3Bucket         string
	S3Region         string
	UploadBaseURL    string
}

func LoadConfig() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, reading configuration from environment variables")
	}

	redisDBStr := getEnv("REDIS_DB", "0")
	redisDB, err := strconv.Atoi(redisDBStr)
	if err != nil {
		log.Printf("Invalid REDIS_DB value '%s', defaulting to 0: %v\n", redisDBStr, err)
		redisDB = 0
	}

	s3Endpoint := getEnv("S3_ENDPOINT", "")
	s3PublicEndpoint := getEnv("S3_PUBLIC_ENDPOINT", s3Endpoint)

	pinataJWT := getEnv("PINATA_JWT", "")
	pinataGateway := getEnv("PINATA_GATEWAY", "")
	if pinataGateway == "" && pinataJWT != "" {
		pinataGateway = discoverPinataGateway(pinataJWT)
		if pinataGateway != "" {
			log.Printf("Auto-discovered Pinata Dedicated Gateway: %s\n", pinataGateway)
		}
	}

	if pinataGateway != "" {
		pinataGateway = strings.TrimPrefix(pinataGateway, "https://")
		pinataGateway = strings.TrimPrefix(pinataGateway, "http://")
		pinataGateway = strings.TrimSuffix(pinataGateway, "/")
	}

	return &Config{
		Port:             ":" + getEnv("PORT", "8080"),
		DBHost:           getEnv("DB_HOST", "localhost"),
		DBPort:           getEnv("DB_PORT", "5432"),
		DBUser:           getEnv("DB_USER", "postgres"),
		DBPassword:       getEnv("DB_PASSWORD", "postgres"),
		DBName:           getEnv("DB_NAME", "veritrace"),
		DBSslMode:        getEnv("DB_SSLMODE", "disable"),
		RedisHost:        getEnv("REDIS_HOST", "localhost"),
		RedisPort:        getEnv("REDIS_PORT", "6379"),
		RedisPassword:    getEnv("REDIS_PASSWORD", ""),
		RedisDB:          redisDB,
		ContractAddress:  getEnv("CONTRACT_ADDRESS", ""),
		QdrantHost:       getEnv("QDRANT_HOST", "localhost"),
		QdrantPort:       getEnv("QDRANT_PORT", "6334"),
		ArbitrumWS:       getEnv("ARBITRUM_SEPOLIA_WS_URL", ""),
		PinataJWT:        pinataJWT,
		PinataGateway:    pinataGateway,
		S3Endpoint:       s3Endpoint,
		S3PublicEndpoint: s3PublicEndpoint,
		S3AccessKey:      getEnv("S3_ACCESS_KEY", ""),
		S3SecretKey:      getEnv("S3_SECRET_KEY", ""),
		S3Bucket:         getEnv("S3_BUCKET", "veritrace"),
		S3Region:         getEnv("S3_REGION", "us-east-1"),
		UploadBaseURL:    getEnv("UPLOAD_BASE_URL", "http://localhost:8080/uploads"),
	}
}

func discoverPinataGateway(jwt string) string {
	if jwt == "" {
		return ""
	}
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	req, err := http.NewRequest("GET", "https://api.pinata.cloud/v3/gateways", nil)
	if err != nil {
		log.Printf("Warning: failed to create Pinata gateway discovery request: %v\n", err)
		return ""
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Warning: failed to fetch Pinata gateways: %v\n", err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Warning: Pinata gateway discovery returned status %d\n", resp.StatusCode)
		return ""
	}

	var result struct {
		Data struct {
			Rows []struct {
				Domain string `json:"domain"`
			} `json:"rows"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("Warning: failed to decode Pinata gateway response: %v\n", err)
		return ""
	}

	if len(result.Data.Rows) > 0 {
		domain := result.Data.Rows[0].Domain
		if domain != "" {
			if !strings.Contains(domain, ".") {
				return domain + ".mypinata.cloud"
			}
			return domain
		}
	}

	return ""
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
