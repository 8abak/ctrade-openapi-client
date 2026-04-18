BEGIN;

CREATE SCHEMA IF NOT EXISTS research;

CREATE TABLE IF NOT EXISTS research.engineering_incident (
    id BIGSERIAL PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'open',
    incident_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'warning',
    fingerprint TEXT NOT NULL,
    summary TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    failure_signature TEXT,
    source_job_id BIGINT,
    source_run_id BIGINT,
    source_decision_id BIGINT,
    affected_services JSONB NOT NULL DEFAULT '[]'::jsonb,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    current_action_id BIGINT,
    resolution JSONB NOT NULL DEFAULT '{}'::jsonb,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    escalated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS research.engineering_action (
    id BIGSERIAL PRIMARY KEY,
    incident_id BIGINT NOT NULL REFERENCES research.engineering_incident (id) ON DELETE CASCADE,
    action_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    rationale TEXT,
    requested_by TEXT NOT NULL,
    requested_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS research.engineering_patch (
    id BIGSERIAL PRIMARY KEY,
    incident_id BIGINT NOT NULL REFERENCES research.engineering_incident (id) ON DELETE CASCADE,
    action_id BIGINT NOT NULL REFERENCES research.engineering_action (id) ON DELETE CASCADE,
    patch_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    target_files JSONB NOT NULL DEFAULT '[]'::jsonb,
    diff_path TEXT,
    backup_path TEXT,
    rollback_path TEXT,
    lines_changed INTEGER NOT NULL DEFAULT 0,
    bytes_changed INTEGER NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research.engineering_smoketest (
    id BIGSERIAL PRIMARY KEY,
    incident_id BIGINT NOT NULL REFERENCES research.engineering_incident (id) ON DELETE CASCADE,
    action_id BIGINT REFERENCES research.engineering_action (id) ON DELETE SET NULL,
    test_name TEXT NOT NULL,
    status TEXT NOT NULL,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS research.engineering_state (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research.engineering_journal (
    id BIGSERIAL PRIMARY KEY,
    component TEXT NOT NULL,
    level TEXT NOT NULL,
    event_type TEXT NOT NULL,
    incident_id BIGINT REFERENCES research.engineering_incident (id) ON DELETE SET NULL,
    action_id BIGINT REFERENCES research.engineering_action (id) ON DELETE SET NULL,
    patch_id BIGINT REFERENCES research.engineering_patch (id) ON DELETE SET NULL,
    message TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS research_engineering_one_active_incident_idx
    ON research.engineering_incident ((1))
    WHERE status IN ('open', 'analyzing', 'executing', 'validating');

CREATE INDEX IF NOT EXISTS research_engineering_incident_status_idx
    ON research.engineering_incident (status, updated_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS research_engineering_incident_fingerprint_idx
    ON research.engineering_incident (fingerprint, updated_at DESC);

CREATE INDEX IF NOT EXISTS research_engineering_action_incident_idx
    ON research.engineering_action (incident_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS research_engineering_action_type_idx
    ON research.engineering_action (action_type, started_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS research_engineering_patch_incident_idx
    ON research.engineering_patch (incident_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS research_engineering_smoketest_incident_idx
    ON research.engineering_smoketest (incident_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS research_engineering_journal_component_idx
    ON research.engineering_journal (component, created_at DESC, id DESC);

INSERT INTO research.engineering_state (key, value)
VALUES (
    'engineering_loop_control',
    '{"enabled": true, "paused": false, "current_incident_id": null, "last_incident_id": null, "last_action_id": null, "last_resolution": null}'::jsonb
)
ON CONFLICT (key) DO NOTHING;

COMMIT;
