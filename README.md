# Streams of GAMMAN

**GOOGL · AAPL · META · MSFT · AMZN · NVDA**

_A local end-to-end data pipeline for processing live and backfilled US stock-market trades._

Streams of GAMMAN consumes live trades from Alpaca, streams them through Redpanda and PyFlink, calculates OHLCV candles and moving averages, and serves the results from PostgreSQL to an interactive Streamlit dashboard.

Normal startup initializes the required local Redpanda topics, `trades.raw` and
`candles`, with one partition and one replica each. This happens before
topic-dependent services start, including when no live trades are arriving.

Historical market sessions can also be requested directly from the dashboard and reconstructed through an Airflow-orchestrated bounded Flink pipeline. When the pipeline starts after the market has opened, it can automatically catch up the current session from market open while live processing continues.

## Architecture

### Live streaming

```text
Alpaca IEX WebSocket
        ↓
Python producer
        ↓
Redpanda — trades.raw
        ↓
PyFlink — event-time processing
        ↓
Redpanda — candles
        ↓
PostgreSQL sink
        ↓
PostgreSQL
        ↓
Streamlit
```

Live trades are processed continuously into **1-minute, 5-minute and 15-minute OHLCV candles**, with **SMA-5 and SMA-20** calculated from the resulting candle streams.

### Historical and catch-up backfills

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
Redpanda — candles
    ↓
PostgreSQL sink
    ↓
PostgreSQL
    ↓
Streamlit
```

Historical sessions can be requested from the dashboard. The same mechanism is used to catch up the current session when the pipeline starts after the market has opened.

For accurate moving averages, the bounded job also processes the preceding completed XNYS trading session as warm-up data. Those warm-up candles establish SMA state but are not emitted as part of the requested session.

Only completed candle windows are emitted during an in-progress catch-up. The continuous Flink job remains responsible for windows that are still forming.

## Technology

| Technology | Role |
|---|---|
| **Alpaca** | Live and historical US stock-market data |
| **Redpanda** | Kafka-compatible event streaming and durable event log |
| **PyFlink** | Event-time processing, deduplication, OHLCV aggregation and moving averages |
| **Apache Airflow** | Historical and current-session backfill orchestration |
| **PostgreSQL** | Serving layer for processed candles |
| **Streamlit / Plotly** | Interactive market dashboard |
| **Docker Compose** | Local infrastructure and service orchestration |
| **Make** | Common build, startup, testing and service-management commands |

## How It Works

Each Alpaca trade receives a deterministic identity based on its feed, symbol and trade ID. The identity is independent of whether the trade arrived through the live or historical path, allowing the same market event to be recognised across both sources.

The continuous Flink job processes live trades using event time and bounded-out-of-orderness watermarks. Trades are deduplicated before being aggregated into 1m, 5m and 15m OHLCV windows. SMA-5 and SMA-20 are maintained as keyed state over the resulting candle streams.

Historical trades present a different problem. By the time they are replayed, their event timestamps are far behind the continuous job's watermark, so simply inserting them into the live event-time pipeline would not reliably reconstruct historical windows.

Airflow therefore coordinates a separate **bounded Flink job** over the relevant section of the Redpanda log. The bounded job reuses the same candle-processing logic while maintaining independent event-time and moving-average state.

For a historical session, Airflow:

1. validates the requested date against the XNYS trading calendar;
2. retrieves the required historical trades from Alpaca;
3. includes the preceding completed trading session as SMA warm-up data;
4. publishes canonical trade events to Redpanda;
5. runs bounded PyFlink reconstruction;
6. emits only completed candles belonging to the requested session;
7. validates the resulting PostgreSQL candles.

The continuous and bounded paths ultimately produce the same candle schema and use the same PostgreSQL serving layer.

PostgreSQL writes use idempotent upserts, allowing historical sessions to be rerun without creating duplicate candle rows.

## Dashboard

The Streamlit dashboard provides:

- selectable GAMMAN ticker;
- 1m, 5m and 15m candle views;
- candlestick charts with SMA-5 and SMA-20;
- live-session metrics;
- historical-session metrics;
- historical session backfills;
- rerunning existing historical sessions;
- automatic current-session catch-up;
- live and historical overlays on a common intraday time axis.

Historical data can be displayed alongside the live session without requiring a separate serving path.

## Running Locally

### Prerequisites

You will need:

- Docker Desktop with Docker Compose;
- `make`;
- Git;
- an Alpaca account and API credentials.

The full stack runs Flink, Airflow, Redpanda, PostgreSQL, Streamlit and several supporting services simultaneously. **At least 8 GB of memory allocated to Docker Desktop is recommended.**

### 1. Clone the repository

```bash
git clone https://github.com/Teqfish/stock-market-streaming-pipeline.git
cd stock-market-streaming-pipeline
```

### 2. Create an Alpaca account and API credentials

This project uses Alpaca's market-data APIs for both live IEX trades and historical trades.

Create an Alpaca Trading API account:

https://app.alpaca.markets/signup

A paper-only account is sufficient for this project; no funded live-trading account is required.

After signing in:

1. select a **Paper Trading** account;
2. open the **API Keys** section of the Alpaca dashboard;
3. generate an API key and secret;
4. save both values securely.

Alpaca only displays the secret key when it is generated. If it is lost, generate a new key pair.

Alpaca's paper-trading documentation is available at:

https://docs.alpaca.markets/docs/paper-trading

### 3. Create the local environment file

Copy the example configuration:

```bash
cp .env.example .env
```

Open `.env` and add your own Alpaca credentials.

For example:

```dotenv
ALPACA_API_KEY=your_alpaca_api_key
ALPACA_SECRET_KEY=your_alpaca_secret_key

AIRFLOW_JWT_SECRET=replace_with_a_random_secret

POSTGRES_DB=stocks
POSTGRES_USER=stocks
POSTGRES_PASSWORD=stocks

TICKERS=GOOGL,AAPL,META,MSFT,AMZN,NVDA
```

Do **not** commit `.env`.

The Alpaca API key and secret belong to your Alpaca account and must remain private.

`AIRFLOW_JWT_SECRET` is used by Airflow to sign and validate API authentication tokens. Generate your own random value with:

```bash
openssl rand -hex 32
```

Copy the resulting value into `.env`:

```dotenv
AIRFLOW_JWT_SECRET=<generated-value>
```

The PostgreSQL password protects only the locally running project database in this configuration, but it should still be kept in `.env` rather than committed to the repository.

Airflow uses its Simple Auth Manager for this local deployment. On the first
`make start`, Airflow generates a random password for the local `airflow` user
and stores it in a private Docker volume. The dashboard reads that volume to
authenticate with the Airflow API. Later starts reuse the same password;
`make reset` removes the volume, so the next start generates a new one. No
Airflow username or password needs to be added to `.env`.

The project is not intended to expose Airflow publicly.

### 4. Build and start the pipeline

Run:

```bash
make start
```

`make start` builds the required images, starts the Docker Compose stack, waits for Flink to become available, submits the continuous PyFlink streaming job and opens the local project interfaces.

The initial build can take several minutes because Docker must download and build the Flink, Airflow and Python environments.

Once startup completes, the main interfaces are:

| Service | URL | Purpose |
|---|---|---|
| **Streamlit** | http://localhost:8501 | Market dashboard and backfill controls |
| **Airflow** | http://localhost:8082 | Backfill DAGs and task monitoring |
| **Flink** | http://localhost:8081 | Streaming and bounded-job monitoring |
| **Redpanda Console** | http://localhost:8080 | Topics, events and consumer inspection |

The live producer uses Alpaca's IEX feed. Outside US market hours there may be no new live trades, but historical sessions can still be reconstructed from the dashboard.

### Useful Make commands

```bash
make start
```

Build and start the complete pipeline and submit the continuous Flink job.

```bash
make ps
```

Show the state of the project's containers.

```bash
make logs
```

Follow Docker Compose logs.

```bash
make test
```

Run the automated test suite inside the project environment.

```bash
make flink-logs
```

Follow Flink JobManager, TaskManager and submitter logs.

```bash
make airflow-logs
```

Follow Airflow API server, scheduler and DAG processor logs.

```bash
make down
```

Stop and remove the project's containers while preserving named-volume data.

```bash
make reset
```

Remove the stack **and its named volumes**. This deletes locally persisted PostgreSQL, Redpanda, Airflow and Flink state and should be used when a completely fresh environment is required.

## Historical Backfills

Historical sessions are normally requested through the Streamlit dashboard.

Choose a ticker and trading date, enable the historical-session view, then select **Run Backfill**. Existing sessions can be reconstructed again using **Rerun Backfill**.

Airflow validates dates against the XNYS exchange calendar, so weekends and exchange holidays are rejected rather than treated as empty trading sessions.

Only one bounded reconstruction is allowed to run at a time. This prevents multiple historical PyFlink jobs from competing for the resources required by the continuously running streaming job.

A bounded reconstruction normally completes in roughly a minute on the development environment, although runtime depends on the amount of trade data and the resources allocated to Docker.

## Current-Session Catch-up

If the pipeline is started after the US market has already opened, enabling the live-session view can trigger a catch-up from the day's market open to the current completed minute.

For example:

```text
09:30                           pipeline starts
  │                                   │
  ├──── bounded catch-up ─────────────┤
                                      ├──── continuous stream ────►
```

The catch-up and live paths converge on the same `candles` topic and PostgreSQL table, so the dashboard presents them as one continuous trading session.

Incomplete 5m or 15m windows are not emitted by the bounded job. They remain the responsibility of the continuously running Flink job and appear once those windows close.

## Testing

The project includes automated tests covering canonical trade-event construction and live/historical event identity, including the requirement that the same Alpaca trade receives the same deterministic identity regardless of ingestion path. Backfill-validation tests cover expected completed candle windows, partial-window exclusion, and detection of missing or unexpected reconstructed windows.

The pipeline has also been exercised end to end across:

- live WebSocket ingestion;
- historical Alpaca retrieval;
- current-session catch-up;
- bounded Flink reconstruction;
- SMA warm-up across trading sessions;
- duplicate/rerun backfills;
- concurrent continuous and bounded Flink processing;
- PostgreSQL upserts and post-backfill validation;
- service shutdown and fresh startup.

Run the automated tests with:

```bash
make test
```

## Design Decisions

**Redpanda as the durable event layer:** Flink outputs return to Redpanda rather than being written directly to PostgreSQL. This separates stream processing from downstream consumers and provides a durable, replayable boundary between processing and serving.

**One canonical trade identity:** `live` and `historical` describe provenance, not identity. The same Alpaca market trade therefore remains the same event regardless of how it entered the pipeline.

**Separate continuous and bounded Flink processing:** Historical events cannot simply be inserted into a continuously advancing event-time pipeline because its watermark has already moved beyond them. Bounded reconstruction allows historical sessions to reuse the processing logic with independent event-time state.

**Warm-up data for stateful indicators:** A requested historical session is processed with data from the preceding completed XNYS session so SMA-5 and SMA-20 are valid from the beginning of the requested session. Warm-up data affects processing state but is not emitted as requested output.

**Redpanda loopback before PostgreSQL:** Processed candle events are written back to Redpanda before a separate sink writes them to PostgreSQL. This keeps Flink focused on stream processing while downstream consumers remain independently replayable.

**Idempotent serving writes:** PostgreSQL uses deterministic candle keys and upserts so a session can be reconstructed again without creating duplicate rows.

**Airflow for bounded work, Flink for stream processing:** Airflow coordinates finite historical jobs and validation while Flink owns event-time transformation. Each tool is used for the workload it is designed to manage.

**Local-first architecture:** The project is intentionally designed to demonstrate streaming, orchestration, stateful processing and replay on a single development machine rather than reproduce a production-scale cloud platform.

## Known Limitations

- The project uses Alpaca's IEX feed rather than consolidated SIP market data.
- Historical candle writes use idempotent PostgreSQL upserts rather than atomic whole-session replacement.
- A reconstruction cannot remove a stale candle if a replacement run produces no event for that window.
- The local Flink cluster is intentionally small and has limited concurrent processing capacity.
- Airflow Simple Auth Manager is suitable for this local demonstration but is not intended as a production authentication configuration.
- Monitoring is provided through the component UIs and logs rather than a dedicated observability/alerting stack.
- The deployment is single-machine and is not designed for high availability.

These are deliberate scope decisions for a portfolio data-engineering project rather than attempts to model a production trading platform.

## Project Status

**Complete.**

Streams of GAMMAN demonstrates an end-to-end local data-engineering system combining live event streaming, event-time processing, durable messaging, stateful aggregation, historical reconstruction, workflow orchestration, validation, persistent serving and interactive analytics.
