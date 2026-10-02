CREATE TABLE IF NOT EXISTS candles (
    symbol          TEXT NOT NULL,
    window_size     TEXT NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    open             DOUBLE PRECISION NOT NULL,
    high             DOUBLE PRECISION NOT NULL,
    low              DOUBLE PRECISION NOT NULL,
    close            DOUBLE PRECISION NOT NULL,
    volume           BIGINT NOT NULL,
    sma_5            DOUBLE PRECISION,
    sma_20           DOUBLE PRECISION,
    PRIMARY KEY (symbol, window_size, window_start)
);

CREATE INDEX IF NOT EXISTS idx_candles_lookup
    ON candles (symbol, window_size, window_start DESC);
