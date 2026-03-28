CREATE TABLE IF NOT EXISTS public.zigmicro (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    starttickid BIGINT NOT NULL,
    endtickid BIGINT NOT NULL,
    confirmtickid BIGINT NOT NULL,
    starttime TIMESTAMPTZ NOT NULL,
    endtime TIMESTAMPTZ NOT NULL,
    confirmtime TIMESTAMPTZ NOT NULL,
    startprice DOUBLE PRECISION NOT NULL,
    endprice DOUBLE PRECISION NOT NULL,
    highprice DOUBLE PRECISION NOT NULL,
    lowprice DOUBLE PRECISION NOT NULL,
    dir INTEGER NOT NULL CHECK (dir IN (-1, 1)),
    tickcount INTEGER NOT NULL CHECK (tickcount >= 1),
    childcount INTEGER NOT NULL DEFAULT 0 CHECK (childcount >= 0),
    dursec DOUBLE PRECISION NOT NULL CHECK (dursec >= 0),
    amplitude DOUBLE PRECISION NOT NULL CHECK (amplitude >= 0),
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed')),
    childstartid BIGINT,
    childendid BIGINT,
    parentid BIGINT,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT zigmicro_identity_uk UNIQUE (symbol, starttickid, endtickid, confirmtickid)
);

CREATE TABLE IF NOT EXISTS public.zigmed (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    starttickid BIGINT NOT NULL,
    endtickid BIGINT NOT NULL,
    confirmtickid BIGINT NOT NULL,
    starttime TIMESTAMPTZ NOT NULL,
    endtime TIMESTAMPTZ NOT NULL,
    confirmtime TIMESTAMPTZ NOT NULL,
    startprice DOUBLE PRECISION NOT NULL,
    endprice DOUBLE PRECISION NOT NULL,
    highprice DOUBLE PRECISION NOT NULL,
    lowprice DOUBLE PRECISION NOT NULL,
    dir INTEGER NOT NULL CHECK (dir IN (-1, 1)),
    tickcount INTEGER NOT NULL CHECK (tickcount >= 1),
    childcount INTEGER NOT NULL DEFAULT 0 CHECK (childcount >= 0),
    dursec DOUBLE PRECISION NOT NULL CHECK (dursec >= 0),
    amplitude DOUBLE PRECISION NOT NULL CHECK (amplitude >= 0),
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed')),
    childstartid BIGINT,
    childendid BIGINT,
    parentid BIGINT,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT zigmed_identity_uk UNIQUE (symbol, starttickid, endtickid, confirmtickid)
);

CREATE TABLE IF NOT EXISTS public.zigmaxi (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    starttickid BIGINT NOT NULL,
    endtickid BIGINT NOT NULL,
    confirmtickid BIGINT NOT NULL,
    starttime TIMESTAMPTZ NOT NULL,
    endtime TIMESTAMPTZ NOT NULL,
    confirmtime TIMESTAMPTZ NOT NULL,
    startprice DOUBLE PRECISION NOT NULL,
    endprice DOUBLE PRECISION NOT NULL,
    highprice DOUBLE PRECISION NOT NULL,
    lowprice DOUBLE PRECISION NOT NULL,
    dir INTEGER NOT NULL CHECK (dir IN (-1, 1)),
    tickcount INTEGER NOT NULL CHECK (tickcount >= 1),
    childcount INTEGER NOT NULL DEFAULT 0 CHECK (childcount >= 0),
    dursec DOUBLE PRECISION NOT NULL CHECK (dursec >= 0),
    amplitude DOUBLE PRECISION NOT NULL CHECK (amplitude >= 0),
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed')),
    childstartid BIGINT,
    childendid BIGINT,
    parentid BIGINT,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT zigmaxi_identity_uk UNIQUE (symbol, starttickid, endtickid, confirmtickid)
);

CREATE TABLE IF NOT EXISTS public.zigmacro (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    starttickid BIGINT NOT NULL,
    endtickid BIGINT NOT NULL,
    confirmtickid BIGINT NOT NULL,
    starttime TIMESTAMPTZ NOT NULL,
    endtime TIMESTAMPTZ NOT NULL,
    confirmtime TIMESTAMPTZ NOT NULL,
    startprice DOUBLE PRECISION NOT NULL,
    endprice DOUBLE PRECISION NOT NULL,
    highprice DOUBLE PRECISION NOT NULL,
    lowprice DOUBLE PRECISION NOT NULL,
    dir INTEGER NOT NULL CHECK (dir IN (-1, 1)),
    tickcount INTEGER NOT NULL CHECK (tickcount >= 1),
    childcount INTEGER NOT NULL DEFAULT 0 CHECK (childcount >= 0),
    dursec DOUBLE PRECISION NOT NULL CHECK (dursec >= 0),
    amplitude DOUBLE PRECISION NOT NULL CHECK (amplitude >= 0),
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed')),
    childstartid BIGINT,
    childendid BIGINT,
    parentid BIGINT,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT zigmacro_identity_uk UNIQUE (symbol, starttickid, endtickid, confirmtickid)
);

CREATE TABLE IF NOT EXISTS public.zigstate (
    id BIGSERIAL PRIMARY KEY,
    jobname TEXT NOT NULL,
    jobtype TEXT NOT NULL CHECK (jobtype IN ('worker', 'backfill')),
    symbol TEXT NOT NULL,
    lasttickid BIGINT NOT NULL DEFAULT 0,
    lasttime TIMESTAMPTZ,
    statejson JSONB NOT NULL DEFAULT '{}'::jsonb,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT zigstate_jobname_uk UNIQUE (jobname)
);

CREATE INDEX IF NOT EXISTS zigmicro_confirm_idx
    ON public.zigmicro (symbol, confirmtickid DESC);

CREATE INDEX IF NOT EXISTS zigmicro_window_idx
    ON public.zigmicro (symbol, endtickid DESC, starttickid DESC);

CREATE INDEX IF NOT EXISTS zigmed_confirm_idx
    ON public.zigmed (symbol, confirmtickid DESC);

CREATE INDEX IF NOT EXISTS zigmed_window_idx
    ON public.zigmed (symbol, endtickid DESC, starttickid DESC);

CREATE INDEX IF NOT EXISTS zigmaxi_confirm_idx
    ON public.zigmaxi (symbol, confirmtickid DESC);

CREATE INDEX IF NOT EXISTS zigmaxi_window_idx
    ON public.zigmaxi (symbol, endtickid DESC, starttickid DESC);

CREATE INDEX IF NOT EXISTS zigmacro_confirm_idx
    ON public.zigmacro (symbol, confirmtickid DESC);

CREATE INDEX IF NOT EXISTS zigmacro_window_idx
    ON public.zigmacro (symbol, endtickid DESC, starttickid DESC);

CREATE INDEX IF NOT EXISTS zigstate_lookup_idx
    ON public.zigstate (jobtype, symbol, updatedat DESC);
