ALTER TABLE content_records DROP COLUMN IF EXISTS media_ipfs_url;
ALTER TABLE content_records DROP COLUMN IF EXISTS media_s3_url;
ALTER TABLE content_records DROP COLUMN IF EXISTS allow_ai_training;
ALTER TABLE content_records DROP COLUMN IF EXISTS media_type;
