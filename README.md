# Streams of  GAMMAN

#### _A local end-to-end data pipeline for processing streaming live and batched historical US stock-market trades._ ####

The pipeline consumes live trades from Alpaca, streams them through Redpanda and PyFlink, calculates OHLCV candles and moving averages, and serves the results from PostgreSQL to an interactive Streamlit dashboard.

Historical market sessions can also be requested from the dashboard and reconstructed through an Airflow-orchestrated backfill pipeline.

## Architecture

### Live streaming

```text
Alpaca WebSocket
      ↓
Python producer
      ↓
Redpanda — trades.raw
      ↓
PyFlink — event-time processing
      ↓
Redpanda — candle topics
      ↓
PostgreSQL sink
      ↓
PostgreSQL
      ↓
Streamlit
```

Live trades are processed continuously into **1-minute, 5-minute and 15-minute OHLCV candles**, with SMA-5 and SMA-20 calculated from the resulting candle streams.

### Historical backfill

```text
Streamlit
    ↓
Airflow
    ↓
Alpaca Historical API
    ↓
Redpanda — trades.raw
    ↓
Bounded PyFlink reconstruction
    ↓
Redpanda — candle topics
    ↓
PostgreSQL
    ↓
Streamlit
```

Backfills allow missing historical trading sessions to be reconstructed without maintaining a separate historical serving path.

## Technology

| Technology | Role |
|---|---|
| **Alpaca** | Live and historical US market data |
| **Redpanda** | Kafka-compatible event streaming and durable logs |
| **PyFlink** | Event-time processing, deduplication and candle aggregation |
| **Airflow** | Historical backfill orchestration |
| **PostgreSQL** | Serving layer for processed candles |
| **Streamlit / Plotly** | Interactive market dashboard |
| **Docker Compose** | Local infrastructure and service orchestration |

## How It Works

Each Alpaca trade receives a deterministic identity based on its feed, symbol, and trade ID. The identity is independent of whether the trade arrived through the live or historical path, allowing duplicate market events to be recognised across both sources.

The continuous Flink job processes live trades using event time and bounded-out-of-orderness watermarks. Trades are deduplicated before being aggregated into 1m, 5m and 15m OHLCV windows.

Historical trades present a different problem: by the time they are replayed, their event timestamps are far behind the continuous job's watermark. Sending them through the normal event-time windows would therefore not reliably reconstruct historical candles.

Airflow instead coordinates a bounded Flink job over the relevant section of the Redpanda log. This reconstructs the requested historical session while leaving the continuous streaming job running independently.

Both paths ultimately produce the same candle events and use the same PostgreSQL serving layer.

## Running Locally

The project runs locally using Docker Compose.

```bash
git clone <repository-url>
cd stock-market-streaming-pipeline

cp .env.example .env
# Add Alpaca API credentials to .env

make start
```

The local environment provides:

- **Streamlit** — market dashboard and historical backfills
- **Airflow** — backfill orchestration
- **Flink** — streaming job monitoring
- **Redpanda Console** — topics and event inspection

> Final startup instructions and service URLs will be confirmed after fresh-clone testing.

## Testing

The project includes automated tests around canonical trade-event creation and identity, including the requirement that the same Alpaca trade receives the same identity through both live and historical ingestion.

The complete pipeline is also tested end-to-end across live ingestion, historical replay, duplicate backfills, concurrent streaming/backfill processing and service recovery.

> This section will be updated following final stress and failure-recovery testing.

## Design Decisions

**Redpanda as the durable event layer:** Flink outputs return to Redpanda rather than being written directly to PostgreSQL. This keeps stream processing separate from downstream consumers and provides a replayable boundary between processing and serving.

**One canonical trade identity:** `live` and `historical` describe provenance, not identity. The same market trade therefore remains the same event regardless of how it entered the pipeline.

**Separate continuous and bounded Flink processing:** Historical events cannot simply be inserted into a continuously advancing event-time pipeline. Bounded reconstruction allows historical sessions to use the same transformation logic without interfering with live watermark state.

**Local-first architecture:** The project is designed to demonstrate streaming and orchestration concepts on a single machine rather than reproduce production-scale cloud infrastructure.

## Known Limitations

- Historical reconstruction currently produces OHLCV candles without rebuilding SMA-5/SMA-20 state across session boundaries.
- Historical candle writes use idempotent PostgreSQL upserts rather than atomic session replacement.
- A reconstruction cannot remove a stale candle if the replacement run produces no event for that window.
- Airflow Simple Auth Manager is used for local authentication.
- The deployment is designed for local demonstration rather than high availability.

These are deliberate scope decisions for the project rather than attempts to model a production trading platform.

## Project Status

The core live and historical pipelines are complete.

Final work consists of failure/recovery testing, repository cleanup and fresh-clone validation before the project is published as a completed portfolio project.
