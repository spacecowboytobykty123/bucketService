CREATE INDEX IF NOT EXISTS idx_bucket_item_bucket_id ON bucket_item(bucket_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bucket_user_id ON bucket(user_id);
