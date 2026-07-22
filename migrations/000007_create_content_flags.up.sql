CREATE TABLE IF NOT EXISTS content_flags (
    id SERIAL PRIMARY KEY,
    sha256_hash VARCHAR(66) NOT NULL REFERENCES content_records(sha256_hash) ON DELETE CASCADE,
    reporter_address VARCHAR(42) NOT NULL,
    reason TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    CONSTRAINT unique_hash_reporter UNIQUE(sha256_hash, reporter_address)
);
