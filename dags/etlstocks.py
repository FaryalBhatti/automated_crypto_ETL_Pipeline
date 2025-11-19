from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
import requests

# Crypto symbol for testing
SYMBOL = "BTC"
SYMBOL_PAIR = SYMBOL.upper() + "USDT"

POSTGRES_CONN_ID = 'postgres_default'

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1)
}

with DAG(
    dag_id='crypto_binance_etl_pipeline',
    default_args=default_args,
    schedule='* * * * *',  # Fetch data every hour
    catchup=False
) as dag:

    @task()
    def extract_stock_data():
        """Extract last 10 1-minute OHLCV candles from Binance API."""
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": SYMBOL_PAIR,
            "interval": "1m",
            "limit": 10
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            candles = response.json()
            data = []
            for entry in candles:
                data.append({
                    "timestamp": int(entry[0] / 1000),
                    "open": float(entry[1]),
                    "high": float(entry[2]),
                    "low": float(entry[3]),
                    "close": float(entry[4]),
                    "volume": float(entry[5])
                })
            return data
        else:
            raise Exception(f"Failed to fetch stock data: {response.status_code}")

    @task()
    def transform_stock_data(data):
        """Transform the extracted stock data by adding a simple 3-period SMA."""
        closes = [row["close"] for row in data]
        sma_list = []
        window = 3
        for i in range(len(closes)):
            if i + 1 < window:
                sma_list.append(None)
            else:
                sma_list.append(sum(closes[i + 1 - window:i + 1]) / window)

        # Add SMA to each row
        transformed = []
        for i, row in enumerate(data):
            transformed.append({
                "timestamp": datetime.utcfromtimestamp(row["timestamp"]),
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "sma": round(sma_list[i], 4) if sma_list[i] is not None else None
            })
        return transformed

    @task()
    def load_stock_data(transformed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_data (
                symbol VARCHAR(10),
                timestamp TIMESTAMP,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT,
                sma FLOAT,
                PRIMARY KEY (symbol, timestamp)
            );
        """)

        for row in transformed_data:
            cursor.execute("""
                INSERT INTO crypto_data (symbol, timestamp, open, high, low, close, volume, sma)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp) DO NOTHING;
            """, (
                SYMBOL,
                row["timestamp"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["sma"]
            ))

        conn.commit()
        cursor.close()
        print(f"Inserted {len(transformed_data)} rows for {SYMBOL} successfully!")

    # DAG workflow
    raw_data = extract_stock_data()
    transformed_data = transform_stock_data(raw_data)
    load_stock_data(transformed_data)
