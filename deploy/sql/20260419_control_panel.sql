BEGIN;

CREATE SCHEMA IF NOT EXISTS research;

ALTER TABLE research.run
    ADD COLUMN IF NOT EXISTS brokerday DATE;

ALTER TABLE research.candidate_result
    ADD COLUMN IF NOT EXISTS setup_fingerprint TEXT;

CREATE INDEX IF NOT EXISTS research_run_brokerday_idx
    ON research.run (brokerday, id DESC);

CREATE INDEX IF NOT EXISTS research_candidate_result_setup_fp_idx
    ON research.candidate_result (setup_fingerprint, run_id DESC, rank ASC);

CREATE TABLE IF NOT EXISTS research.candidate_library (
    setup_fingerprint TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'active',
    promoted BOOLEAN NOT NULL DEFAULT FALSE,
    operator_notes TEXT NOT NULL DEFAULT '',
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research.control_operator_action (
    id BIGSERIAL PRIMARY KEY,
    actor TEXT NOT NULL,
    action_type TEXT NOT NULL,
    scope TEXT NOT NULL,
    target_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS research_control_operator_action_created_idx
    ON research.control_operator_action (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS research_control_operator_action_scope_idx
    ON research.control_operator_action (scope, created_at DESC, id DESC);

INSERT INTO research.state (key, value)
VALUES (
    'control_mission',
    jsonb_build_object(
        'missionTitle', 'Find tradable entry setups one broker day at a time',
        'mainObjective', 'Find candidate entry points first, then discover similarities among winners, test them on same-day holdout and prior-day context, and continue until the best justified entry-only result or a real final verdict exists.',
        'tradableDefinition', 'Similar positions, properties, indicators, and setup structures that move at least 2x current spread fast enough to get risk-free early, with the highest sustainable accuracy.',
        'scoringPriority', 'accuracy > speed > stability > frequency',
        'currentPhase', 'entry-only',
        'allowedDirections', jsonb_build_array(
            'find strong candidate entry points',
            'cluster similarities among winning entries',
            'prefer low-spread regimes when evidence supports it',
            'validate on same-day holdout before accepting strength'
        ),
        'forbiddenDirections', jsonb_build_array(
            'live trading or execution changes',
            'hold/exit logic expansion before entry quality is proven',
            'unbounded brute force scans',
            'manual guardrail bypasses'
        ),
        'minimumRunsBeforeStop', 5,
        'sameDayHoldoutRequired', true,
        'priorDayValidationRequired', false,
        'preferredSideLock', 'both',
        'guidanceNotes', 'Entry-only first. Seek regimes that reach 2x spread quickly enough to reduce risk early. Do not stop on one weak run when bounded next directions remain.'
    )
)
ON CONFLICT (key) DO NOTHING;

INSERT INTO research.state (key, value)
VALUES (
    'control_panel_settings',
    jsonb_build_object(
        'researchLoopEnabled', true,
        'engineeringLoopEnabled', true,
        'maxRetriesPerIncident', 3,
        'maxNextJobs', 4,
        'maxPatchFiles', 2,
        'maxPatchLineChanges', 120,
        'maxPatchBytes', 20000,
        'restartRateLimitPerHour', 6,
        'failedDirectionStopCount', 3,
        'iterationBudget', 8,
        'approvedSliceLadder', jsonb_build_array(5000, 10000, 20000, 40000),
        'approvedCandidateFamilies', jsonb_build_object(
            'threshold_grid', true,
            'pair_combo', true,
            'triad_combo', true,
            'contrast_gate', true,
            'regime_split', true,
            'tighten_winner', true,
            'slice_expand', true,
            'side_locked_refine', true
        ),
        'researchModelOverride', '',
        'engineeringModelOverride', ''
    )
)
ON CONFLICT (key) DO NOTHING;

COMMIT;
