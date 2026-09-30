import os
import json
import urllib.error
import urllib.request
import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from plotly.subplots import make_subplots

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

GAMMAN_SYMBOLS = [
    "GOOGL",
    "AAPL",
    "META",
    "MSFT",
    "AMZN",
    "NVDA",
]

AIRFLOW_API_URL = os.getenv(
    "AIRFLOW_API_URL",
    "http://airflow-api-server:8080",
)
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORDS_FILE = "/run/secrets/airflow_passwords.json"

NEW_YORK_TZ = ZoneInfo("America/New_York")

def get_current_market_date():
    return datetime.now(NEW_YORK_TZ).date()


st.set_page_config(
    page_title="Streams of GAMMAN",
    page_icon="📈",
    layout="wide",
)

def get_airflow_token():
    """Authenticate with Airflow and return a JWT access token."""
    with open(AIRFLOW_PASSWORDS_FILE) as f:
        passwords = json.load(f)

    payload = json.dumps(
        {
            "username": AIRFLOW_USERNAME,
            "password": passwords[AIRFLOW_USERNAME],
        }
    ).encode()

    request = urllib.request.Request(
        f"{AIRFLOW_API_URL}/auth/token",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)["access_token"]


def trigger_backfill(symbol, trading_date):
    """Trigger the Airflow market_backfill DAG."""
    token = get_airflow_token()

    payload = json.dumps(
        {
            "logical_date": datetime.now(timezone.utc).isoformat(),
            "conf": {
                "symbols": [symbol],
                "trading_date": trading_date.isoformat(),
            },
        }
    ).encode()

    request = urllib.request.Request(
        f"{AIRFLOW_API_URL}/api/v2/dags/market_backfill/dagRuns",
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def get_backfill_runs(limit=20):
    """Return recent market_backfill DAG runs from Airflow."""
    token = get_airflow_token()

    request = urllib.request.Request(
        (
            f"{AIRFLOW_API_URL}/api/v2/dags/market_backfill/dagRuns"
            f"?limit={limit}&order_by=-logical_date"
        ),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        method="GET",
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        payload = json.load(response)

    runs = []

    for dag_run in payload.get("dag_runs", []):
        conf = dag_run.get("conf") or {}
        symbols = conf.get("symbols") or []
        trading_date = conf.get("trading_date")

        if not symbols or not trading_date:
            continue

        runs.append(
            {
                "dag_run_id": dag_run["dag_run_id"],
                "symbol": symbols[0],
                "trading_date": date.fromisoformat(trading_date),
                "state": dag_run.get("state", "unknown"),
                "logical_date": dag_run.get("logical_date"),
            }
        )

    return runs


def get_active_backfill(backfill_runs, symbol, trading_date):
    """Return an active run for this symbol/date, if one exists."""
    active_states = {"queued", "running"}

    for run in backfill_runs:
        if (
            run["symbol"] == symbol
            and run["trading_date"] == trading_date
            and run["state"] in active_states
        ):
            return run

    return None


def session_exists(symbol, session_date):
    """Return whether regular-session 1m candles exist locally."""
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM candles
            WHERE symbol = %s
              AND window_size = '1m'
              AND (window_start AT TIME ZONE 'America/New_York')::date = %s
              AND (window_start AT TIME ZONE 'America/New_York')::time
                    >= TIME '09:30'
              AND (window_start AT TIME ZONE 'America/New_York')::time
                    < TIME '16:00'
        );
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (symbol, session_date))
            return cursor.fetchone()[0]


def live_session_needs_backfill(symbol, session_date):
    """
    Return whether today's local candle data is missing the market open.

    A live session needs catch-up when no 1m candles exist for the session,
    or when its earliest candle starts after 09:30 New York time.
    """
    query = """
        SELECT MIN(
            (window_start AT TIME ZONE 'America/New_York')::time
        )
        FROM candles
        WHERE symbol = %s
          AND window_size = '1m'
          AND (window_start AT TIME ZONE 'America/New_York')::date = %s
          AND (window_start AT TIME ZONE 'America/New_York')::time
                >= TIME '09:30'
          AND (window_start AT TIME ZONE 'America/New_York')::time
                < TIME '16:00';
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (symbol, session_date))
            earliest_candle = cursor.fetchone()[0]

    if earliest_candle is None:
        return True

    return earliest_candle > pd.Timestamp("09:30:00").time()


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def get_symbols():
    """Return the symbols supported by the dashboard."""
    return GAMMAN_SYMBOLS.copy()


def get_candles(symbol, window_size, limit=200):
    query = """
        SELECT
            symbol,
            window_size,
            window_start,
            window_end,
            open,
            high,
            low,
            close,
            volume,
            sma_5,
            sma_20
        FROM candles
        WHERE symbol = %s
          AND window_size = %s
        ORDER BY window_start DESC
        LIMIT %s;
    """

    with get_connection() as connection:
        dataframe = pd.read_sql_query(
            query,
            connection,
            params=(symbol, window_size, limit),
        )

    return dataframe.sort_values("window_start")


def get_available_sessions(symbol):
    query = """
        SELECT DISTINCT
            (window_start AT TIME ZONE 'America/New_York')::date
                AS session_date
        FROM candles
        WHERE symbol = %s
          AND window_size = '1m'
          AND (window_start AT TIME ZONE 'America/New_York')::time
                >= TIME '09:30'
          AND (window_start AT TIME ZONE 'America/New_York')::time
                < TIME '16:00'
        ORDER BY session_date DESC;
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (symbol,))
            return [row[0] for row in cursor.fetchall()]


def get_candles_for_session(symbol, window_size, session_date):
    query = """
        SELECT
            symbol,
            window_size,
            window_start,
            window_end,
            open,
            high,
            low,
            close,
            volume,
            sma_5,
            sma_20
        FROM candles
        WHERE symbol = %s
          AND window_size = %s
          AND (window_start AT TIME ZONE 'America/New_York')::date = %s
          AND (window_start AT TIME ZONE 'America/New_York')::time
                >= TIME '09:30'
          AND (window_start AT TIME ZONE 'America/New_York')::time
                < TIME '16:00'
        ORDER BY window_start;
    """

    with get_connection() as connection:
        return pd.read_sql_query(
            query,
            connection,
            params=(symbol, window_size, session_date),
        )


def get_session_metrics(symbol, session_date):
    query = """
        SELECT
            (ARRAY_AGG(
                open ORDER BY window_start
            ))[1] AS session_open,
            (ARRAY_AGG(
                close ORDER BY window_start DESC
            ))[1] AS latest_price,
            MAX(high) AS session_high,
            MIN(low) AS session_low,
            SUM(volume) AS session_volume
        FROM candles
        WHERE symbol = %s
        AND window_size = '1m'
        AND (window_start AT TIME ZONE 'America/New_York')::date = %s
        AND (window_start AT TIME ZONE 'America/New_York')::time
                >= TIME '09:30'
        AND (window_start AT TIME ZONE 'America/New_York')::time
                < TIME '16:00';
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (symbol, session_date))
            row = cursor.fetchone()

    if row is None or row[0] is None:
        return None

    return {
        "session_open": row[0],
        "latest_price": row[1],
        "session_high": row[2],
        "session_low": row[3],
        "session_volume": row[4],
    }


def create_market_chart(
    live_dataframe=None,
    historical_dataframe=None,
    historical_date=None,
):
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
    )

    reference_date = pd.Timestamp("2000-01-01")

    def add_plot_time(dataframe):
        dataframe = dataframe.copy()

        market_time = (
            pd.to_datetime(
                dataframe["window_start"],
                utc=True,
            )
            .dt.tz_convert("America/New_York")
        )

        dataframe["plot_time"] = (
            reference_date
            + pd.to_timedelta(market_time.dt.hour, unit="h")
            + pd.to_timedelta(market_time.dt.minute, unit="m")
            + pd.to_timedelta(market_time.dt.second, unit="s")
        )

        return dataframe

    # --------------------------------------
    # Historical session — background layer
    # --------------------------------------

    if (
        historical_dataframe is not None
        and not historical_dataframe.empty
    ):
        historical = add_plot_time(historical_dataframe)

        historical_volume_colors = [
            "rgba(70, 130, 255, 0.35)"
            if close >= open_
            else "rgba(145, 80, 210, 0.35)"
            for open_, close in zip(
                historical["open"],
                historical["close"],
            )
        ]

        historical_label = (
            historical_date.strftime("%d %b")
            if historical_date
            else "Historical"
        )

        figure.add_trace(
            go.Candlestick(
                x=historical["plot_time"],
                open=historical["open"],
                high=historical["high"],
                low=historical["low"],
                close=historical["close"],
                name=f"{historical_label} price",
                increasing_line_color="rgba(70, 130, 255, 0.55)",
                decreasing_line_color="rgba(145, 80, 210, 0.55)",
                increasing_fillcolor="rgba(70, 130, 255, 0.35)",
                decreasing_fillcolor="rgba(145, 80, 210, 0.35)",
                opacity=0.65,
                whiskerwidth=0.66,
            ),
            row=1,
            col=1,
        )

        figure.add_trace(
            go.Scatter(
                x=historical["plot_time"],
                y=historical["sma_5"],
                mode="lines",
                name=f"{historical_label} SMA 5",
                line=dict(
                    color="rgba(100, 160, 255, 0.55)",
                ),
                connectgaps=False,
            ),
            row=1,
            col=1,
        )

        figure.add_trace(
            go.Scatter(
                x=historical["plot_time"],
                y=historical["sma_20"],
                mode="lines",
                name=f"{historical_label} SMA 20",
                line=dict(
                    color="rgba(150, 100, 220, 0.50)",
                ),
                connectgaps=False,
            ),
            row=1,
            col=1,
        )

        figure.add_trace(
            go.Bar(
                x=historical["plot_time"],
                y=historical["volume"],
                name=f"{historical_label} volume",
                marker_color=historical_volume_colors,
            ),
            row=2,
            col=1,
        )

    # -------------------------------
    # Live session — foreground layer
    # -------------------------------

    if live_dataframe is not None and not live_dataframe.empty:
        live = add_plot_time(live_dataframe)

        live_volume_colors = [
            "#096009"
            if close >= open_
            else "#600909"
            for open_, close in zip(
                live["open"],
                live["close"],
            )
        ]

        figure.add_trace(
            go.Candlestick(
                x=live["plot_time"],
                open=live["open"],
                high=live["high"],
                low=live["low"],
                close=live["close"],
                name="Live price",
                increasing_line_color="#007700",
                decreasing_line_color="#770000",
                increasing_fillcolor="#096009",
                decreasing_fillcolor="#600909",
                opacity=1,
                whiskerwidth=0.66,
            ),
            row=1,
            col=1,
        )

        figure.add_trace(
            go.Scatter(
                x=live["plot_time"],
                y=live["sma_5"],
                mode="lines",
                name="Live SMA 5",
                line=dict(color="#BE7A26"),
                opacity=0.9,
                connectgaps=False,
            ),
            row=1,
            col=1,
        )

        figure.add_trace(
            go.Scatter(
                x=live["plot_time"],
                y=live["sma_20"],
                mode="lines",
                name="Live SMA 20",
                line=dict(color="#D1511E"),
                opacity=0.7,
                connectgaps=False,
            ),
            row=1,
            col=1,
        )

        figure.add_trace(
            go.Bar(
                x=live["plot_time"],
                y=live["volume"],
                name="Live volume",
                marker_color=live_volume_colors,
                opacity=0.6,
            ),
            row=2,
            col=1,
        )

    figure.update_layout(
        height=750,
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_rangeslider_visible=False,
        barmode="overlay",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="left",
            x=0,
        ),
    )

    figure.update_xaxes(
        type="date",
        tickformat="%H:%M",
        title_text="New York market time",
        row=2,
        col=1,
    )

    return figure


## ======================================
## RENDER DASHBOARD
## ======================================

st.html('''
            <header>
                <h1 style="font-size: 3em;margin-bottom: 10px;" align="center">
                    Streams of GAMMAN
                </h1>
                <p style="font-size: 0.9em;margin-bottom: 40px;" align="center";>
                    Real-time streaming pipeline for top US tech share prices
                </p>
            </header>
        ''')

symbols = get_symbols()

if not symbols:
    st.warning("No candle data is currently available.")
    st.stop()

controls = st.columns([1, 1.4, 1, 1, 1])

with controls[0]:
    symbol = st.selectbox(
        "Ticker",
        symbols,
        index=0,
    )

with controls[1]:
    session_date = st.date_input(
        "Historical session",
        value=date.today(),
        max_value=date.today(),
    )

with controls[2]:
    show_live_session = st.checkbox(
        "Show live session",
        value=False,
    )

with controls[2]:
    show_historical_session = st.checkbox(
        "Show historical session",
        value=True,
    )

with controls[3]:
    window_size = st.segmented_control(
        "Window",
        options=["1m", "5m", "15m"],
        default="1m",
        required=True,
    )


@st.fragment(run_every="10s")
def render_backfill_controls(
    symbol,
    session_date,
    show_live_session,
    show_historical_session,
):
    live_date = get_current_market_date()

    try:
        backfill_runs = get_backfill_runs()
    except Exception as exc:
        st.warning(f"Could not retrieve Airflow backfill status: {exc}")
        backfill_runs = []

    # --------------------------------------
    # Automatic live-session catch-up
    # --------------------------------------

    if show_live_session:
        live_backfill = get_active_backfill(
            backfill_runs,
            symbol,
            live_date,
        )

        if live_backfill is None:
            try:
                needs_live_backfill = live_session_needs_backfill(
                    symbol,
                    live_date,
                )

                if needs_live_backfill:
                    trigger_backfill(
                        symbol,
                        live_date,
                    )

                    st.info(
                        f"Recovering today's earlier session for {symbol}. "
                        "Live candles will continue to update while the "
                        "backfill runs."
                    )

            except urllib.error.HTTPError as exc:
                error_body = exc.read().decode()
                st.error(
                    f"Airflow rejected the live catch-up request "
                    f"(HTTP {exc.code}): {error_body}"
                )

            except Exception as exc:
                st.error(
                    f"Could not start live-session catch-up: {exc}"
                )

        else:
            if live_backfill["state"] == "running":
                st.info(
                    f"Recovering today's earlier session for {symbol}. "
                    "Live candles will continue to update while the "
                    "backfill runs."
                )
            else:
                st.info(
                    f"Today's live-session catch-up for {symbol} "
                    "is queued."
                )

    # --------------------------------------
    # Manual historical-session backfill
    # --------------------------------------

    local_session_exists = session_exists(
        symbol,
        session_date,
    )

    active_backfill = get_active_backfill(
        backfill_runs,
        symbol,
        session_date,
    )

    if show_historical_session:
        if active_backfill is not None:
            state = active_backfill["state"]

            if state == "running":
                st.info(
                    f"Backfill in progress for {symbol} on "
                    f"{session_date.strftime('%a %d %b %Y')}. "
                    "Historical data normally takes about 45 seconds "
                    "to become available."
                )
            else:
                st.info(
                    f"Backfill queued for {symbol} on "
                    f"{session_date.strftime('%a %d %b %Y')}."
                )

        else:
            if local_session_exists:
                button_label = "Rerun Backfill"
            else:
                st.info(
                    f"No local data is available for {symbol} on "
                    f"{session_date.strftime('%a %d %b %Y')}."
                )
                button_label = "Run Backfill"

            if st.button(
                button_label,
                key=f"backfill_{symbol}_{session_date.isoformat()}",
            ):
                try:
                    trigger_backfill(
                        symbol,
                        session_date,
                    )

                    if local_session_exists:
                        st.success(
                            f"Backfill rerun requested for {symbol} on "
                            f"{session_date.strftime('%a %d %b %Y')}."
                        )
                    else:
                        st.success(
                            f"Backfill requested for {symbol} on "
                            f"{session_date.strftime('%a %d %b %Y')}."
                        )

                    st.rerun(scope="fragment")

                except urllib.error.HTTPError as exc:
                    error_body = exc.read().decode()
                    st.error(
                        f"Airflow rejected the backfill request "
                        f"(HTTP {exc.code}): {error_body}"
                    )

                except Exception as exc:
                    st.error(f"Could not start backfill: {exc}")

    # --------------------------------------
    # Backfill queue
    # --------------------------------------

    active_runs = [
        run
        for run in backfill_runs
        if run["state"] in {"queued", "running"}
    ]

    if active_runs:
        running_count = sum(
            run["state"] == "running"
            for run in active_runs
        )
        queued_count = sum(
            run["state"] == "queued"
            for run in active_runs
        )

        with st.expander(
            f"Backfills ({running_count} running, {queued_count} queued)"
        ):
            queue_data = pd.DataFrame(
                [
                    {
                        "Status": run["state"].title(),
                        "Ticker": run["symbol"],
                        "Trading session": run[
                            "trading_date"
                        ].strftime("%a %d %b %Y"),
                    }
                    for run in active_runs
                ]
            )

            st.dataframe(
                queue_data,
                width="stretch",
                hide_index=True,
            )


render_backfill_controls(
    symbol,
    session_date,
    show_live_session,
    show_historical_session,
)


@st.fragment(run_every="10s")
def render_dashboard(
    symbol,
    window_size,
    historical_date,
    show_live_session,
    show_historical_session,
):
    live_date = get_current_market_date()

    live_candles = pd.DataFrame()
    historical_candles = pd.DataFrame()

    live_metrics = None
    historical_metrics = None

    if show_live_session:
        live_candles = get_candles_for_session(
            symbol=symbol,
            window_size=window_size,
            session_date=live_date,
        )

        live_metrics = get_session_metrics(
            symbol,
            live_date,
        )

    if show_historical_session:
        historical_candles = get_candles_for_session(
            symbol=symbol,
            window_size=window_size,
            session_date=historical_date,
        )

        historical_metrics = get_session_metrics(
            symbol,
            historical_date,
        )

    # --------------------------------------
    # Live scorecards
    # --------------------------------------

    if show_live_session:
        if live_metrics is None:
            st.info(
                f"No live regular-session data is currently available "
                f"for {symbol}."
            )
        else:
            latest_price = live_metrics["latest_price"]
            session_open = live_metrics["session_open"]

            price_change = latest_price - session_open
            price_change_percent = (
                price_change / session_open * 100
                if session_open
                else None
            )

            live_columns = st.columns([3,1,1,1,1])

            live_columns[0].subheader(
                f"{live_date.strftime('%a, %d %b %Y')}"
                f" - TODAY",
                divider="orange",
                )

            live_columns[1].metric(
                "Latest price",
                f"${latest_price:,.2f}",
                (
                    f"{price_change:+.2f} "
                    f"({price_change_percent:+.2f}%)"
                ),
            )

            live_columns[2].metric(
                "High",
                f"${live_metrics['session_high']:,.2f}",
            )

            live_columns[3].metric(
                "Low",
                f"${live_metrics['session_low']:,.2f}",
            )

            live_columns[4].metric(
                "Volume",
                f"{live_metrics['session_volume']:,.0f}",
            )

    # --------------------------------------
    # Historical scorecards
    # --------------------------------------

    if show_historical_session and historical_metrics is not None:
        historical_close = historical_metrics["latest_price"]
        historical_open = historical_metrics["session_open"]

        price_change = historical_close - historical_open
        price_change_percent = (
            price_change / historical_open * 100
            if historical_open
            else None
        )

        historical_columns = st.columns([3, 1, 1, 1, 1])

        historical_columns[0].subheader(
            f"{historical_date.strftime('%a, %d %b %Y')}",
            divider="blue",
        )

        historical_columns[1].metric(
            "Close",
            f"${historical_close:,.2f}",
            (
                f"{price_change:+.2f} "
                f"({price_change_percent:+.2f}%)"
            ),
        )

        historical_columns[2].metric(
            "High",
            f"${historical_metrics['session_high']:,.2f}",
        )

        historical_columns[3].metric(
            "Low",
            f"${historical_metrics['session_low']:,.2f}",
        )

        historical_columns[4].metric(
            "Volume",
            f"{historical_metrics['session_volume']:,.0f}",
        )

    # --------------------------------------
    # Chart
    # --------------------------------------

    has_live_data = not live_candles.empty
    has_historical_data = not historical_candles.empty

    if has_live_data or has_historical_data:
        st.plotly_chart(
            create_market_chart(
                live_dataframe=(
                    live_candles
                    if has_live_data
                    else None
                ),
                historical_dataframe=(
                    historical_candles
                    if has_historical_data
                    else None
                ),
                historical_date=historical_date,
            ),
            width="stretch",
        )

    elif not show_live_session and not show_historical_session:
        st.info(
            "Enable the live or historical session to display market data."
        )

    # --------------------------------------
    # Raw candle data
    # --------------------------------------

    if has_live_data or has_historical_data:
        with st.expander("Candle data"):
            if has_live_data:
                st.caption(
                    f"Live — {live_date.strftime('%a %d %b %Y')}"
                )
                st.dataframe(
                    live_candles.sort_values(
                        "window_start",
                        ascending=False,
                    ),
                    width="stretch",
                    hide_index=True,
                )

            if has_historical_data:
                st.caption(
                    f"Historical — "
                    f"{historical_date.strftime('%a %d %b %Y')}"
                )
                st.dataframe(
                    historical_candles.sort_values(
                        "window_start",
                        ascending=False,
                    ),
                    width="stretch",
                    hide_index=True,
                )


render_dashboard(
    symbol,
    window_size,
    session_date,
    show_live_session,
    show_historical_session,
)
