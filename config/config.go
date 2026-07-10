package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	Port            string
	DBHost          string
	DBPort          string
	DBUser          string
	DBPassword      string
	DBName          string
	DBSslMode       string
	RedisHost       string
	RedisPort       string
	RedisPassword   string
	RedisDB         int
	ContractAddress string
	QdrantHost      string
	QdrantPort      string
	ArbitrumWS      string
	PinataJWT       string
	S3Endpoint      string
	S3PublicEndpoint string
	S3AccessKey     string
	S3SecretKey     string
	S3Bucket        string
	S3Region        string
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

	return &Config{
		Port:            ":" + getEnv("PORT", "8080"),
		DBHost:          getEnv("DB_HOST", "localhost"),
		DBPort:          getEnv("DB_PORT", "5432"),
		DBUser:          getEnv("DB_USER", "postgres"),
		DBPassword:      getEnv("DB_PASSWORD", "postgres"),
		DBName:          getEnv("DB_NAME", "veritrace"),
		DBSslMode:       getEnv("DB_SSLMODE", "disable"),
		RedisHost:       getEnv("REDIS_HOST", "localhost"),
		RedisPort:       getEnv("REDIS_PORT", "6379"),
		RedisPassword:   getEnv("REDIS_PASSWORD", ""),
		RedisDB:         redisDB,
		ContractAddress: getEnv("CONTRACT_ADDRESS", ""),
		QdrantHost:      getEnv("QDRANT_HOST", "localhost"),
		QdrantPort:      getEnv("QDRANT_PORT", "6334"),
		ArbitrumWS:      getEnv("ARBITRUM_SEPOLIA_WS_URL", ""),
		PinataJWT:       getEnv("PINATA_JWT", ""),
		S3Endpoint:      s3Endpoint,
		S3PublicEndpoint: s3PublicEndpoint,
		S3AccessKey:     getEnv("S3_ACCESS_KEY", ""),
		S3SecretKey:     getEnv("S3_SECRET_KEY", ""),
		S3Bucket:        getEnv("S3_BUCKET", "veritrace"),
		S3Region:        getEnv("S3_REGION", "us-east-1"),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
