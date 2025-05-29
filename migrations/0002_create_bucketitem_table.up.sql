CREATE TABLE IF NOT EXISTS bucket_item (
    id BIGSERIAL PRIMARY KEY,
    bucket_id BIGINT NOT NULL REFERENCES bucket(id) ON DELETE CASCADE,
    toy_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(bucket_id, toy_id)
);