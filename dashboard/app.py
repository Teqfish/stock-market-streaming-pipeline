import os

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st


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

st.set_page_config(
    page_title="Streams of GAMMAN",
    page_icon="📈",
    layout="wide",
)


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


def get_latest_timestamp(symbol, window_size):
    query = """
        SELECT MAX(window_start)
        FROM candles
        WHERE symbol = %s
          AND window_size = %s;
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (symbol, window_size))
            return cursor.fetchone()[0]


def get_candles_for_view(symbol, window_size, view):
    latest_timestamp = get_latest_timestamp(symbol, window_size)

    if latest_timestamp is None:
        return pd.DataFrame()

    if view == "Live session":
        query = """
            WITH latest_session AS (
                SELECT
                    (MAX(window_start) AT TIME ZONE 'America/New_York')::date
                    AS session_date
                FROM candles
                WHERE symbol = %s
                  AND window_size = %s
            )
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
            FROM candles, latest_session
            WHERE symbol = %s
              AND window_size = %s
              AND (window_start AT TIME ZONE 'America/New_York')::date
                    = session_date
              AND (window_start AT TIME ZONE 'America/New_York')::time
                    >= TIME '09:30'
              AND (window_start AT TIME ZONE 'America/New_York')::time
                    < TIME '16:00'
            ORDER BY window_start;
        """

        params = (
            symbol,
            window_size,
            symbol,
            window_size,
        )

    else:
        lookbacks = {
            "1h": pd.Timedelta(hours=1),
            "4h": pd.Timedelta(hours=4),
            "1d": pd.Timedelta(days=1),
            "5d": pd.Timedelta(days=5),
        }

        start_timestamp = latest_timestamp - lookbacks[view]

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
              AND window_start >= %s
            ORDER BY window_start;
        """

        params = (
            symbol,
            window_size,
            start_timestamp,
        )

    with get_connection() as connection:
        return pd.read_sql_query(
            query,
            connection,
            params=params,
        )


def get_session_metrics(symbol):
    query = """
        WITH latest_session AS (
            SELECT
                (MAX(window_start) AT TIME ZONE 'America/New_York')::date
                AS session_date
            FROM candles
            WHERE symbol = %s
              AND window_size = '1m'
        )
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
        FROM candles, latest_session
        WHERE symbol = %s
          AND window_size = '1m'
          AND (window_start AT TIME ZONE 'America/New_York')::date
                = session_date
          AND (window_start AT TIME ZONE 'America/New_York')::time
                >= TIME '09:30'
          AND (window_start AT TIME ZONE 'America/New_York')::time
                < TIME '16:00';
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (symbol, symbol))
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

def create_price_chart(dataframe, hide_non_trading=False):
    figure = go.Figure()

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
            whiskerwidth=0.66
        )
    )

    # figure.add_trace(
    #     go.Scatter(
    #         x=dataframe["window_start"],
    #         y=dataframe["close"],
    #         mode="lines",
    #         name="Close",
    #         line=dict(color=COLOR_CLOSE),
    #         opacity=0.3,
    #     )
    # )

    figure.add_trace(
        go.Scatter(
            x=dataframe["window_start"],
            y=dataframe["sma_5"],
            mode="lines",
            name="SMA 5",
            line=dict(color="#359EFF"),
            opacity=0.9,
        )
    )

    figure.add_trace(
        go.Scatter(
            x=dataframe["window_start"],
            y=dataframe["sma_20"],
            mode="lines",
            name="SMA 20",
            line=dict(color="#1E36D1"),
            opacity=0.7,
        )
    )

    figure.update_layout(
        xaxis_rangeslider_visible=False,
        height=600,
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.1,
            xanchor="left",
            x=0,
        ),
    )

    if hide_non_trading:
        figure.update_xaxes(
            rangebreaks=[
                dict(bounds=["sat", "mon"]),
                dict(bounds=[16, 9.5], pattern="hour"),
            ]
        )

    return figure


def create_volume_chart(dataframe, hide_non_trading=False):
    figure = go.Figure()

    figure.add_trace(
        go.Bar(
            x=dataframe["window_start"],
            y=dataframe["volume"],
            name="Volume",
            marker_color='deepskyblue',
            opacity=0.6
        )
    )

    figure.update_layout(
        height=250,
        margin=dict(l=20, r=20, t=20, b=20),
        showlegend=False,
    )

    if hide_non_trading:
        figure.update_xaxes(
            rangebreaks=[
                dict(bounds=["sat", "mon"]),
                dict(bounds=[16, 9.5], pattern="hour"),
            ]
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

controls = st.columns([2, 2, 3, 3])

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

with controls[2]:
    view = st.segmented_control(
        "View",
        options=["Live session", "1h", "4h", "1d", "5d"],
        default="Live session",
        required=True,
    )

@st.fragment(run_every="10s")
def render_dashboard(symbol, window_size, view):
    candles = get_candles_for_view(
        symbol=symbol,
        window_size=window_size,
        view=view,
    )

    hide_non_trading = view in {"1d", "5d"}

    session_metrics = get_session_metrics(symbol)

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
        create_price_chart(
            candles,
            hide_non_trading=hide_non_trading,
        ),
        width="stretch",
    )

    st.plotly_chart(
        create_volume_chart(
            candles,
            hide_non_trading=hide_non_trading,
        ),
        width="stretch",
    )

    with st.expander("Candle data"):
        st.dataframe(
            candles.sort_values("window_start", ascending=False),
            width="stretch",
            hide_index=True,
        )


render_dashboard(symbol, window_size, view)
