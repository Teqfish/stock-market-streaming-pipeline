import os
import json
import urllib.error
import urllib.request
import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from datetime import date, datetime, timezone
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


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def get_symbols():
    query = """
        SELECT DISTINCT symbol
        FROM candles;
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            available_symbols = [row[0] for row in cursor.fetchall()]

    return sorted(
        available_symbols,
        key=lambda symbol: (
            symbol not in GAMMAN_SYMBOLS,
            GAMMAN_SYMBOLS.index(symbol)
            if symbol in GAMMAN_SYMBOLS
            else symbol,
        ),
    )


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


def create_market_chart(dataframe):
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
    )

    figure.add_trace(
        go.Candlestick(
            x=dataframe["window_start"],
            open=dataframe["open"],
            high=dataframe["high"],
            low=dataframe["low"],
            close=dataframe["close"],
            name="Price",
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
            x=dataframe["window_start"],
            y=dataframe["sma_5"],
            mode="lines",
            name="SMA 5",
            line=dict(color="#359EFF"),
            opacity=0.9,
        ),
        row=1,
        col=1,
    )

    figure.add_trace(
        go.Scatter(
            x=dataframe["window_start"],
            y=dataframe["sma_20"],
            mode="lines",
            name="SMA 20",
            line=dict(color="#1E36D1"),
            opacity=0.7,
        ),
        row=1,
        col=1,
    )

    figure.add_trace(
        go.Bar(
            x=dataframe["window_start"],
            y=dataframe["volume"],
            name="Volume",
            marker_color="deepskyblue",
            opacity=0.6,
        ),
        row=2,
        col=1,
    )

    figure.update_layout(
        height=750,
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_rangeslider_visible=False,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="left",
            x=0,
        ),
    )

    return figure

## ======================================
## RENDER DASHBOARD
## ======================================

st.title("Streams of GAMMAN")
st.caption("Real-time stock market streaming pipeline")

symbols = get_symbols()

if not symbols:
    st.warning("No candle data is currently available.")
    st.stop()

controls = st.columns(4)

with controls[0]:
    symbol = st.selectbox(
        "Ticker",
        symbols,
        index=0,
    )

with controls[1]:
    window_size = st.segmented_control(
        "Window",
        options=["1m", "5m", "15m"],
        default="1m",
        required=True,
    )

available_sessions = get_available_sessions(symbol)

if not available_sessions:
    st.warning(f"No trading sessions are available for {symbol}.")
    st.stop()

with controls[2]:
    session_date = st.selectbox(
        "Trading session",
        available_sessions,
        index=0,
        format_func=lambda value: value.strftime("%a %d %b %Y"),
    )

with controls[3]:
    cal_date = st.date_input("Trading Date","today")

backfiller = st.columns(2)

with backfiller[0]:
    with st.expander("Backfill historical data"):
        st.caption(
            "Recover historical market data for a completed trading session. "
            "Backfills are limited to one trading day per run."
        )

        backfill_col_1, backfill_col_2 = st.columns(2)

        with backfill_col_1:
            backfill_symbol = st.selectbox(
                "Ticker",
                symbols,
                key="backfill_symbol",
            )

        with backfill_col_2:
            backfill_date = st.date_input(
                "Trading date",
                value=date.today(),
                max_value=date.today(),
                key="backfill_date",
            )

        if st.button("Run backfill"):
            try:
                result = trigger_backfill(
                    backfill_symbol,
                    backfill_date,
                )

                st.success(
                    f"Backfill started for {backfill_symbol} "
                    f"on {backfill_date.isoformat()}."
                )

            except urllib.error.HTTPError as exc:
                error_body = exc.read().decode()
                st.error(
                    f"Airflow rejected the backfill request "
                    f"(HTTP {exc.code}): {error_body}"
                )

            except Exception as exc:
                st.error(f"Could not start backfill: {exc}")


@st.fragment(run_every="10s")
def render_dashboard(symbol, window_size, session_date):
    candles = get_candles_for_session(
        symbol=symbol,
        window_size=window_size,
        session_date=session_date,
    )

    session_metrics = get_session_metrics(
        symbol,
        session_date,
    )

    if candles.empty:
        st.info(f"No {window_size} candles are available for {symbol}.")
        return

    if session_metrics is None:
        st.warning("No regular-session data is currently available.")
        return

    latest_price = session_metrics["latest_price"]
    session_open = session_metrics["session_open"]

    price_change = latest_price - session_open
    price_change_percent = (
        price_change / session_open * 100
        if session_open
        else None
    )

    metric_columns = st.columns(4)

    metric_columns[0].metric(
        "Latest price",
        f"${latest_price:,.2f}",
        f"{price_change:+.2f} ({price_change_percent:+.2f}%)",
    )

    metric_columns[1].metric(
        "Session high",
        f"${session_metrics['session_high']:,.2f}",
    )

    metric_columns[2].metric(
        "Session low",
        f"${session_metrics['session_low']:,.2f}",
    )

    metric_columns[3].metric(
        "IEX session volume",
        f"{session_metrics['session_volume']:,.0f}",
    )

    st.plotly_chart(
        create_market_chart(candles),
        width="stretch",
    )

    with st.expander("Candle data"):
        st.dataframe(
            candles.sort_values("window_start", ascending=False),
            width="stretch",
            hide_index=True,
        )


render_dashboard(symbol, window_size, session_date)
