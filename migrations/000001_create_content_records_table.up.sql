CREATE TABLE IF NOT EXISTS content_records (
    sha256_hash VARCHAR(66) PRIMARY KEY,
    creator_address VARCHAR(42) NOT NULL,
    phash BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    ipfs_cid TEXT NOT NULL,
    ai_tool TEXT NOT NULL
);
