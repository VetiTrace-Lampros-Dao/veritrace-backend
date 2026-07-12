package content

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	s3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type StorageProvider interface {
	UploadFile(ctx context.Context, reader io.Reader, filename string, contentType string) (string, error)
}

type LocalStorageProvider struct {
	uploadDir string
	baseUrl   string
}

func NewLocalStorageProvider(uploadDir, baseUrl string) *LocalStorageProvider {
	return &LocalStorageProvider{
		uploadDir: uploadDir,
		baseUrl:   baseUrl,
	}
}

func (p *LocalStorageProvider) UploadFile(ctx context.Context, reader io.Reader, filename string, contentType string) (string, error) {
	if err := os.MkdirAll(p.uploadDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create upload directory: %w", err)
	}

	destPath := filepath.Join(p.uploadDir, filename)
	destFile, err := os.Create(destPath)
	if err != nil {
		return "", fmt.Errorf("failed to create file on disk: %w", err)
	}
	defer destFile.Close()

	if _, err := io.Copy(destFile, reader); err != nil {
		return "", fmt.Errorf("failed to copy file bytes: %w", err)
	}

	return fmt.Sprintf("%s/%s", p.baseUrl, filename), nil
}

type S3StorageProvider struct {
	client         *s3.Client
	bucket         string
	endpoint       string
	publicEndpoint string
	region         string
}

func NewS3StorageProvider(ctx context.Context, cfg *config.Config) (*S3StorageProvider, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if cfg.S3Endpoint != "" {
			return aws.Endpoint{
				URL:           cfg.S3Endpoint,
				SigningRegion: cfg.S3Region,
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	awsCfg, err := s3Config.LoadDefaultConfig(ctx,
		s3Config.WithRegion(cfg.S3Region),
		s3Config.WithEndpointResolverWithOptions(customResolver),
		s3Config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.S3AccessKey,
			cfg.S3SecretKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load S3 SDK configuration: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &S3StorageProvider{
		client:         s3Client,
		bucket:         cfg.S3Bucket,
		endpoint:       cfg.S3Endpoint,
		publicEndpoint: cfg.S3PublicEndpoint,
		region:         cfg.S3Region,
	}, nil
}

func (p *S3StorageProvider) UploadFile(ctx context.Context, reader io.Reader, filename string, contentType string) (string, error) {
	_, err := p.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(p.bucket),
		Key:         aws.String(filename),
		Body:        reader,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload object to S3: %w", err)
	}

	if p.publicEndpoint != "" {
		return fmt.Sprintf("%s/%s/%s", p.publicEndpoint, p.bucket, filename), nil
	} else if p.endpoint != "" {
		return fmt.Sprintf("%s/%s/%s", p.endpoint, p.bucket, filename), nil
	}

	return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", p.bucket, p.region, filename), nil
}

func InitStorageProvider(ctx context.Context, cfg *config.Config) (StorageProvider, error) {
	if cfg.S3AccessKey != "" && cfg.S3SecretKey != "" {
		provider, err := NewS3StorageProvider(ctx, cfg)
		if err != nil {
			return nil, err
		}

		s3Client := provider.client
		_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(cfg.S3Bucket),
		})
		if err != nil {
		}

		policy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [
				{
					"Sid": "PublicRead",
					"Effect": "Allow",
					"Principal": "*",
					"Action": ["s3:GetObject"],
					"Resource": ["arn:aws:s3:::%s/*"]
				}
			]
		}`, cfg.S3Bucket)

		_, err = s3Client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
			Bucket: aws.String(cfg.S3Bucket),
			Policy: aws.String(policy),
		})
		if err != nil {
			log.Printf("Storage warning: failed to apply public read-only bucket policy: %v", err)
		}

		return provider, nil
	}

	return NewLocalStorageProvider("./uploads", cfg.UploadBaseURL), nil
}
