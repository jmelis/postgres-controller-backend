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

-- Resources: one row per object. Three lifecycle states:
--   live (deletion_timestamp IS NULL),
--   dying (deletion_timestamp set, has finalizers),
--   fully deleted / tombstone (deletion_timestamp set, no finalizers).
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

-- Covers live-only queries (e.g., Get). List uses a broader predicate that
-- also includes dying objects (deletion_timestamp set, has finalizers) and
-- falls back to idx_resources_watch.
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

-- Fenced write stored procedure.
-- Performs fence check, optional no-op suppression, counter increment,
-- and upsert in a single server-side call. Does NOT issue pg_notify —
-- the caller fires the doorbell after commit to avoid the global
-- notification-queue lock that serializes all commits.
-- Returns per-step timings (microseconds) so the caller can emit them
-- as Prometheus histograms without additional round-trips.
DROP FUNCTION IF EXISTS pgctl_write;
CREATE OR REPLACE FUNCTION pgctl_write(
    p_domain          TEXT,
    p_gvk             TEXT,
    p_namespace        TEXT,
    p_name             TEXT,
    p_bucket_id        INT,
    p_holder           TEXT,
    p_epoch            BIGINT,
    p_expected_version BIGINT,
    p_force_write      BOOLEAN,
    p_spec             JSONB,
    p_status           JSONB,
    p_metadata         JSONB,
    p_deletion_ts      TIMESTAMPTZ DEFAULT NULL
) RETURNS TABLE(out_uid UUID, out_version BIGINT, out_seq BIGINT, out_changed BOOLEAN,
                out_fence_us BIGINT, out_suppress_us BIGINT, out_counter_us BIGINT, out_upsert_us BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    v_seq         BIGINT;
    v_uid         UUID;
    v_version     BIGINT;
    v_existing    RECORD;
    v_t0          TIMESTAMPTZ;
    v_fence_us    BIGINT := 0;
    v_suppress_us BIGINT := 0;
    v_counter_us  BIGINT := 0;
    v_upsert_us   BIGINT := 0;
BEGIN
    -- 1. Fence check
    v_t0 := clock_timestamp();
    PERFORM 1 FROM bucket_leases
    WHERE bucket_id = p_bucket_id
      AND domain    = p_domain
      AND holder    = p_holder
      AND epoch     = p_epoch
      AND expires_at > now()
    FOR SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'fence violation' USING ERRCODE = 'P0001';
    END IF;
    v_fence_us := extract(microseconds from clock_timestamp() - v_t0)::BIGINT;

    -- 2. Suppression check (skip if force_write)
    v_t0 := clock_timestamp();
    IF NOT p_force_write THEN
        SELECT kr.uid, kr.object_version, kr.spec, kr.status, kr.metadata, kr.deletion_timestamp
          INTO v_existing
          FROM kubernetes_resources kr
         WHERE kr.gvk = p_gvk AND kr.namespace = p_namespace AND kr.name = p_name;

        IF FOUND THEN
            IF p_domain = 'status' THEN
                IF v_existing.status = p_status THEN
                    v_suppress_us := extract(microseconds from clock_timestamp() - v_t0)::BIGINT;
                    RETURN QUERY SELECT v_existing.uid, v_existing.object_version, 0::BIGINT, false,
                        v_fence_us, v_suppress_us, v_counter_us, v_upsert_us;
                    RETURN;
                END IF;
            ELSE
                IF v_existing.spec = p_spec
                   AND (p_status IS NULL OR v_existing.status = p_status)
                   AND v_existing.metadata = p_metadata
                   AND v_existing.deletion_timestamp IS NOT DISTINCT FROM p_deletion_ts THEN
                    v_suppress_us := extract(microseconds from clock_timestamp() - v_t0)::BIGINT;
                    RETURN QUERY SELECT v_existing.uid, v_existing.object_version, 0::BIGINT, false,
                        v_fence_us, v_suppress_us, v_counter_us, v_upsert_us;
                    RETURN;
                END IF;
            END IF;
        END IF;
    END IF;
    v_suppress_us := extract(microseconds from clock_timestamp() - v_t0)::BIGINT;

    -- 3. Counter increment
    v_t0 := clock_timestamp();
    INSERT INTO gvk_bucket_counters (bucket_id, gvk, current_seq)
    VALUES (p_bucket_id, p_gvk, 1)
    ON CONFLICT (bucket_id, gvk)
    DO UPDATE SET current_seq = gvk_bucket_counters.current_seq + 1
    RETURNING current_seq INTO v_seq;
    v_counter_us := extract(microseconds from clock_timestamp() - v_t0)::BIGINT;

    -- 4. Upsert
    v_t0 := clock_timestamp();
    IF p_domain = 'status' THEN
        IF p_expected_version = 0 THEN
            RAISE EXCEPTION 'WriteStatus requires ExpectedVersion > 0' USING ERRCODE = 'P0004';
        END IF;

        UPDATE kubernetes_resources
           SET gvk_bucket_seq = v_seq,
               object_version = object_version + 1,
               status         = p_status,
               updated_at     = now()
         WHERE gvk = p_gvk AND namespace = p_namespace AND name = p_name
           AND object_version = p_expected_version
        RETURNING uid, object_version INTO v_uid, v_version;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'conflict' USING ERRCODE = 'P0002';
        END IF;
    ELSIF p_expected_version = 0 THEN
        BEGIN
            INSERT INTO kubernetes_resources
                (gvk, namespace, name, bucket_id, gvk_bucket_seq,
                 object_version, spec, status, metadata, deletion_timestamp)
            VALUES (p_gvk, p_namespace, p_name, p_bucket_id, v_seq,
                    1, p_spec, p_status, p_metadata, p_deletion_ts)
            RETURNING uid, object_version INTO v_uid, v_version;
        EXCEPTION WHEN unique_violation THEN
            -- Tombstone revival: if the conflicting row is fully deleted
            -- (deletion_timestamp set, no finalizers), overwrite it as a
            -- fresh resource with a new UID. Dying objects (have finalizers)
            -- and live objects fall through to 'already exists'.
            UPDATE kubernetes_resources
               SET uid                = gen_random_uuid(),
                   bucket_id          = p_bucket_id,
                   gvk_bucket_seq     = v_seq,
                   object_version     = 1,
                   spec               = p_spec,
                   status             = COALESCE(p_status, '{}'::jsonb),
                   metadata           = p_metadata,
                   deletion_timestamp = NULL,
                   created_at         = now(),
                   updated_at         = now()
             WHERE gvk = p_gvk AND namespace = p_namespace AND name = p_name
               AND deletion_timestamp IS NOT NULL
               AND (metadata->'finalizers' IS NULL OR metadata->'finalizers' = '[]'::jsonb) -- tombstone filter: also in list.go, compactor.go, writer.go
            RETURNING uid, object_version INTO v_uid, v_version;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'already exists' USING ERRCODE = 'P0003';
            END IF;
        END;
    ELSE
        UPDATE kubernetes_resources
           SET gvk_bucket_seq     = v_seq,
               object_version     = object_version + 1,
               spec               = p_spec,
               status             = COALESCE(p_status, status),
               metadata           = p_metadata,
               deletion_timestamp = p_deletion_ts,
               updated_at         = now()
         WHERE gvk = p_gvk AND namespace = p_namespace AND name = p_name
           AND object_version = p_expected_version
        RETURNING uid, object_version INTO v_uid, v_version;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'conflict' USING ERRCODE = 'P0002';
        END IF;
    END IF;
    v_upsert_us := extract(microseconds from clock_timestamp() - v_t0)::BIGINT;

    RETURN QUERY SELECT v_uid, v_version, v_seq, true,
        v_fence_us, v_suppress_us, v_counter_us, v_upsert_us;
END;
$$;
