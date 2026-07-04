-- DynamoDB stream checkpoint per (stream, shard), fenced by bucket lease
CREATE TABLE IF NOT EXISTS stream_checkpoints (
    stream_arn   TEXT        NOT NULL,
    shard_id     TEXT        NOT NULL,
    last_seq_num TEXT        NOT NULL,
    holder_id    TEXT        NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_arn, shard_id)
);

-- MC-to-bucket registry; mc_index is never reused
CREATE TABLE IF NOT EXISTS mc_registry (
    mc_id           TEXT PRIMARY KEY,
    mc_index        INT  NOT NULL UNIQUE,
    read_table_arn  TEXT NOT NULL,
    read_stream_arn TEXT,
    state           TEXT NOT NULL CHECK (state IN ('active', 'draining', 'retired')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
