-- Gapless monotonic sequence per (bucket, GVK)
CREATE TABLE IF NOT EXISTS gvk_bucket_counters (
    bucket_id   INT    NOT NULL,
    gvk         TEXT   NOT NULL,
    current_seq BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_id, gvk)
) WITH (fillfactor = 50);

-- Lease fencing: authoritative writer epoch per (bucket, domain)
CREATE TABLE IF NOT EXISTS bucket_leases (
    bucket_id  INT    NOT NULL,
    domain     TEXT   NOT NULL CHECK (domain IN ('spec', 'status')),
    holder     TEXT   NOT NULL,
    epoch      BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (bucket_id, domain)
);

-- Resources: one live row per object + tombstones
CREATE TABLE IF NOT EXISTS kubernetes_resources (
    gvk                TEXT        NOT NULL,
    namespace          TEXT        NOT NULL,
    name               TEXT        NOT NULL,
    uid                UUID        NOT NULL DEFAULT gen_random_uuid(),
    bucket_id          INT         NOT NULL,
    gvk_bucket_seq     BIGINT      NOT NULL,
    object_version     BIGINT      NOT NULL DEFAULT 1,
    spec               JSONB       NOT NULL,
    status             JSONB       NOT NULL,
    metadata           JSONB       NOT NULL,
    deletion_timestamp TIMESTAMPTZ NULL,
    created_at         TIMESTAMPTZ DEFAULT now(),
    updated_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (gvk, namespace, name)
);

CREATE INDEX IF NOT EXISTS idx_resources_list
    ON kubernetes_resources (gvk, bucket_id)
    WHERE deletion_timestamp IS NULL;

CREATE INDEX IF NOT EXISTS idx_resources_watch
    ON kubernetes_resources (gvk, bucket_id, gvk_bucket_seq);

-- Failover epoch
CREATE TABLE IF NOT EXISTS cluster_epoch (
    singleton   BOOL PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    timeline_id BIGINT NOT NULL
);

INSERT INTO cluster_epoch (timeline_id) VALUES (1) ON CONFLICT DO NOTHING;

-- Compaction horizon per (bucket, GVK)
CREATE TABLE IF NOT EXISTS compaction_horizon (
    bucket_id     INT    NOT NULL,
    gvk           TEXT   NOT NULL,
    compacted_seq BIGINT NOT NULL,
    PRIMARY KEY (bucket_id, gvk)
);
