CREATE TABLE IF NOT EXISTS sync_checkpoints (
    key VARCHAR(50) PRIMARY KEY,
    last_value BIGINT NOT NULL
);
INSERT INTO sync_checkpoints (key, last_value) VALUES ('evm_listener', 286145000) ON CONFLICT DO NOTHING;
