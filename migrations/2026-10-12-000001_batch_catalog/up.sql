-- Keep old and new application replicas correct throughout the migration: wait
-- for existing writers, then install maintenance and backfill in this transaction.
-- Reads can continue. No foreign key to the OPTIONAL batch/completion record.
LOCK TABLE task IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE batch_catalog (
    batch_id uuid PRIMARY KEY,
    first_created_at timestamptz NOT NULL,
    last_created_at timestamptz NOT NULL,
    CHECK (first_created_at <= last_created_at)
);

INSERT INTO batch_catalog
SELECT batch_id, MIN(created_at), MAX(created_at)
FROM task WHERE batch_id IS NOT NULL GROUP BY batch_id;

CREATE INDEX idx_batch_catalog_page
    ON batch_catalog (first_created_at DESC, batch_id DESC);
-- Normally empty: API creation gives every task the same transaction timestamp.
-- Retention can skip the extrema recomputation for all uniform batches.
CREATE INDEX idx_batch_catalog_mixed ON batch_catalog (batch_id)
    WHERE first_created_at <> last_created_at;

CREATE FUNCTION catalog_inserted_tasks() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- One group per inserted batch, not one trigger invocation per task. Multiple
    -- dedupe runs in the same POST do not rewrite an unchanged catalog record.
    INSERT INTO batch_catalog AS c (batch_id, first_created_at, last_created_at)
    SELECT batch_id, MIN(created_at), MAX(created_at)
    FROM inserted_tasks WHERE batch_id IS NOT NULL
    GROUP BY batch_id ORDER BY batch_id
    ON CONFLICT (batch_id) DO UPDATE
    SET first_created_at = LEAST(c.first_created_at, EXCLUDED.first_created_at),
        last_created_at = GREATEST(c.last_created_at, EXCLUDED.last_created_at)
    WHERE EXCLUDED.first_created_at < c.first_created_at
       OR EXCLUDED.last_created_at > c.last_created_at;
    RETURN NULL;
END;
$$;

CREATE TRIGGER task_catalog_insert
AFTER INSERT ON task REFERENCING NEW TABLE AS inserted_tasks
FOR EACH STATEMENT EXECUTE FUNCTION catalog_inserted_tasks();

CREATE FUNCTION catalog_deleted_tasks() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    -- A9: retention has already acquired its whole task set. Serialize only
    -- retention of the SAME batches, in UUID order. Lifecycle UPDATEs never
    -- acquire these locks. Keep this separate from the reads below: READ COMMITTED
    -- needs a fresh snapshot after waiting for another retention transaction.
    PERFORM c.batch_id FROM batch_catalog c
    WHERE c.batch_id IN (SELECT batch_id FROM deleted_tasks)
    ORDER BY c.batch_id FOR UPDATE OF c;

    DELETE FROM batch_catalog c
    WHERE c.batch_id IN (SELECT batch_id FROM deleted_tasks)
      AND NOT EXISTS (SELECT 1 FROM task t WHERE t.batch_id = c.batch_id);

    -- Uniform batches only need the existence probe above. Recompute extrema
    -- for heterogeneous batches only; avoid updating unchanged rows/indexes.
    UPDATE batch_catalog c
    SET first_created_at = live.first_created_at,
        last_created_at = live.last_created_at
    FROM (
        SELECT previous.batch_id, dates.*
        FROM batch_catalog previous
        CROSS JOIN LATERAL (
            SELECT MIN(t.created_at) AS first_created_at,
                   MAX(t.created_at) AS last_created_at
            FROM task t WHERE t.batch_id = previous.batch_id
        ) dates
        WHERE previous.first_created_at <> previous.last_created_at
          AND previous.batch_id IN (SELECT batch_id FROM deleted_tasks)
    ) live
    WHERE c.batch_id = live.batch_id
      AND (c.first_created_at, c.last_created_at)
          IS DISTINCT FROM (live.first_created_at, live.last_created_at);
    RETURN NULL;
END;
$$;

CREATE TRIGGER task_catalog_delete
AFTER DELETE ON task REFERENCING OLD TABLE AS deleted_tasks
FOR EACH STATEMENT EXECUTE FUNCTION catalog_deleted_tasks();

-- task.batch_id and task.created_at are immutable in the application. There is
-- deliberately no UPDATE trigger: claims, heartbeats and terminal transitions
-- must neither copy task rows into transition tables nor write/lock the catalog.
ANALYZE batch_catalog;
