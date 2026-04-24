BEGIN;

CREATE TABLE IF NOT EXISTS public.motionmodelscenario (
    id BIGSERIAL PRIMARY KEY,
    scenarioname TEXT NOT NULL,
    signalrule TEXT NOT NULL,
    family TEXT,
    min_efficiency3 DOUBLE PRECISION,
    min_spreadmultiple3 DOUBLE PRECISION,
    max_spreadmultiple3 DOUBLE PRECISION,
    require_state10 TEXT,
    require_state30 TEXT,
    allow_state3 TEXT[],
    velocity10_ratio_max DOUBLE PRECISION,
    cooldownsec INTEGER,
    riskfreeusd DOUBLE PRECISION,
    targetusd DOUBLE PRECISION,
    stopusd DOUBLE PRECISION,
    lookaheadsec INTEGER,
    isactive BOOLEAN NOT NULL DEFAULT TRUE,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS motionmodelscenario_signalrule_uidx
    ON public.motionmodelscenario (signalrule);

CREATE INDEX IF NOT EXISTS motionmodelscenario_isactive_family_idx
    ON public.motionmodelscenario (isactive, family, scenarioname);

CREATE TABLE IF NOT EXISTS public.motionmodelresult (
    id BIGSERIAL PRIMARY KEY,
    scenarioid BIGINT REFERENCES public.motionmodelscenario (id),
    signalrule TEXT,
    fromts TIMESTAMPTZ,
    tots TIMESTAMPTZ,
    signals INTEGER,
    targets INTEGER,
    riskfree INTEGER,
    stops INTEGER,
    nodecision INTEGER,
    targetpct DOUBLE PRECISION,
    usefulpct DOUBLE PRECISION,
    stoppct DOUBLE PRECISION,
    avgsecondstoriskfree DOUBLE PRECISION,
    avgmaxadverse DOUBLE PRECISION,
    avgscore DOUBLE PRECISION,
    profitproxy DOUBLE PRECISION,
    passedconstraints BOOLEAN,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS motionmodelresult_scenarioid_createdat_idx
    ON public.motionmodelresult (scenarioid, createdat DESC);

CREATE INDEX IF NOT EXISTS motionmodelresult_signalrule_fromts_tots_idx
    ON public.motionmodelresult (signalrule, fromts, tots);

WITH family_seed AS (
    SELECT *
    FROM (
        VALUES
            (
                'micro_burst_choppy',
                'micro_burst_choppy',
                'choppy',
                'choppy',
                ARRAY['fast_up', 'building_up', 'fast_down', 'building_down']::TEXT[],
                0.60::DOUBLE PRECISION
            ),
            (
                'micro_burst_short_confirm',
                'micro_burst_short_confirm',
                'choppy',
                NULL::TEXT,
                ARRAY['fast_up', 'building_up', 'fast_down', 'building_down']::TEXT[],
                0.75::DOUBLE PRECISION
            ),
            (
                'continuation',
                'continuation',
                NULL::TEXT,
                NULL::TEXT,
                ARRAY['fast_up', 'building_up', 'fast_down', 'building_down']::TEXT[],
                NULL::DOUBLE PRECISION
            ),
            (
                'strict_micro_burst',
                'strict_micro_burst',
                'choppy',
                'choppy',
                ARRAY['fast_up', 'fast_down']::TEXT[],
                0.50::DOUBLE PRECISION
            )
    ) AS seeded(
        family,
        scenario_prefix,
        require_state10,
        require_state30,
        allow_state3,
        velocity10_ratio_max
    )
),
efficiency_seed AS (
    SELECT value::DOUBLE PRECISION AS min_efficiency3
    FROM (VALUES (0.55), (0.60), (0.65)) AS valueset(value)
),
spread_seed AS (
    SELECT *
    FROM (
        VALUES
            (2.5::DOUBLE PRECISION, 5.0::DOUBLE PRECISION, '2p5_5'),
            (3.0::DOUBLE PRECISION, 5.0::DOUBLE PRECISION, '3_5'),
            (3.0::DOUBLE PRECISION, 7.0::DOUBLE PRECISION, '3_7')
    ) AS valueset(min_spreadmultiple3, max_spreadmultiple3, spread_label)
),
cooldown_seed AS (
    SELECT value::INTEGER AS cooldownsec
    FROM (VALUES (10), (20), (30)) AS valueset(value)
),
riskfree_seed AS (
    SELECT *
    FROM (
        VALUES
            (0.20::DOUBLE PRECISION, '020'),
            (0.30::DOUBLE PRECISION, '030'),
            (0.40::DOUBLE PRECISION, '040')
    ) AS valueset(riskfreeusd, riskfree_label)
),
target_seed AS (
    SELECT *
    FROM (
        VALUES
            (0.70::DOUBLE PRECISION, '070'),
            (1.00::DOUBLE PRECISION, '100')
    ) AS valueset(targetusd, target_label)
),
stop_seed AS (
    SELECT *
    FROM (
        VALUES
            (0.70::DOUBLE PRECISION, '070'),
            (1.00::DOUBLE PRECISION, '100')
    ) AS valueset(stopusd, stop_label)
),
seed_rows AS (
    SELECT
        format(
            '%s eff=%.2f sm=%.1f-%.1f cooldown=%s rf=%.2f target=%.2f stop=%.2f',
            family_seed.scenario_prefix,
            efficiency_seed.min_efficiency3,
            spread_seed.min_spreadmultiple3,
            spread_seed.max_spreadmultiple3,
            cooldown_seed.cooldownsec,
            riskfree_seed.riskfreeusd,
            target_seed.targetusd,
            stop_seed.stopusd
        ) AS scenarioname,
        format(
            'scenario_%s_e%s_sm%s_cd%s_rf%s_t%s_s%s',
            family_seed.scenario_prefix,
            lpad((round(efficiency_seed.min_efficiency3 * 100))::INTEGER::TEXT, 3, '0'),
            spread_seed.spread_label,
            cooldown_seed.cooldownsec,
            riskfree_seed.riskfree_label,
            target_seed.target_label,
            stop_seed.stop_label
        ) AS signalrule,
        family_seed.family,
        efficiency_seed.min_efficiency3,
        spread_seed.min_spreadmultiple3,
        spread_seed.max_spreadmultiple3,
        family_seed.require_state10,
        family_seed.require_state30,
        family_seed.allow_state3,
        family_seed.velocity10_ratio_max,
        cooldown_seed.cooldownsec,
        riskfree_seed.riskfreeusd,
        target_seed.targetusd,
        stop_seed.stopusd,
        300::INTEGER AS lookaheadsec,
        TRUE AS isactive
    FROM family_seed
    CROSS JOIN efficiency_seed
    CROSS JOIN spread_seed
    CROSS JOIN cooldown_seed
    CROSS JOIN riskfree_seed
    CROSS JOIN target_seed
    CROSS JOIN stop_seed
)
INSERT INTO public.motionmodelscenario (
    scenarioname,
    signalrule,
    family,
    min_efficiency3,
    min_spreadmultiple3,
    max_spreadmultiple3,
    require_state10,
    require_state30,
    allow_state3,
    velocity10_ratio_max,
    cooldownsec,
    riskfreeusd,
    targetusd,
    stopusd,
    lookaheadsec,
    isactive
)
SELECT
    scenarioname,
    signalrule,
    family,
    min_efficiency3,
    min_spreadmultiple3,
    max_spreadmultiple3,
    require_state10,
    require_state30,
    allow_state3,
    velocity10_ratio_max,
    cooldownsec,
    riskfreeusd,
    targetusd,
    stopusd,
    lookaheadsec,
    isactive
FROM seed_rows
ON CONFLICT (signalrule) DO UPDATE SET
    scenarioname = EXCLUDED.scenarioname,
    family = EXCLUDED.family,
    min_efficiency3 = EXCLUDED.min_efficiency3,
    min_spreadmultiple3 = EXCLUDED.min_spreadmultiple3,
    max_spreadmultiple3 = EXCLUDED.max_spreadmultiple3,
    require_state10 = EXCLUDED.require_state10,
    require_state30 = EXCLUDED.require_state30,
    allow_state3 = EXCLUDED.allow_state3,
    velocity10_ratio_max = EXCLUDED.velocity10_ratio_max,
    cooldownsec = EXCLUDED.cooldownsec,
    riskfreeusd = EXCLUDED.riskfreeusd,
    targetusd = EXCLUDED.targetusd,
    stopusd = EXCLUDED.stopusd,
    lookaheadsec = EXCLUDED.lookaheadsec,
    isactive = EXCLUDED.isactive;

COMMIT;
