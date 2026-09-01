# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7cc77d9e-3c0b-4f75-806c-4374d799079c",
# META       "default_lakehouse_name": "ctrader_lakehouse",
# META       "default_lakehouse_workspace_id": "38721b31-0da3-4aa4-9150-0deacd89ed23",
# META       "known_lakehouses": [
# META         {
# META           "id": "7cc77d9e-3c0b-4f75-806c-4374d799079c"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "442ba72b-5074-aca0-4c14-3e0d15463a4b",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "mirrored_db": {}
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import requests
import json
import logging
from datetime import datetime, timedelta

# ---------------------------------------------------------
# START SPARK SESSION AT THE VERY TOP (Fabric-friendly)
# ---------------------------------------------------------
# spark = SparkSession.builder.getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleAlpacaPipeline:
    """
    Bronze Layer Pipeline:
    Store RAW Alpaca API JSON responses as a single column (json_raw)
    """

    def __init__(self, api_key: str, secret_key: str):
        self.spark = spark
        self.api_key = api_key
        self.secret_key = secret_key

        self.base_url = "https://data.alpaca.markets"
        self.headers = {
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": secret_key,
            "accept": "application/json"
        }

        self.top_stocks = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
            "META", "NVDA", "JPM", "V", "WMT"
        ]

        self.top_crypto = [
            "SOL/USD", "XRP/USD", "ADA/USD", "DOGE/USD", "DOT/USD",
            "MATIC/USD", "LINK/USD", "AVAX/USD", "UNI/USD", "ATOM/USD"
        ]

        self.timeframe = "5Min"

        logger.info("Pipeline initialized (Bronze RAW JSON mode).")

    # ---------------------------------------------------------
    # RAW FETCH FUNCTIONS (NO TRANSFORMATION)
    # ---------------------------------------------------------

    def fetch_stock_bars(self, symbol: str, start: str, end: str):
        try:
            url = f"{self.base_url}/v2/stocks/bars"
            params = {
                "symbols": symbol,
                "timeframe": self.timeframe,
                "start": start,
                "end": end,
                "limit": 10000,
                "adjustment": "raw",
                "sort": "asc"
            }

            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error(f"Error fetching stock bars for {symbol}: {e}")
            return {}

    def fetch_crypto_bars(self, symbol: str, start: str, end: str):
        try:
            clean_symbol = symbol.replace("/USD", "")
            url = f"{self.base_url}/v1beta1/crypto/bars"
            params = {
                "symbols": clean_symbol,
                "timeframe": self.timeframe,
                "start": start,
                "end": end,
                "limit": 10000,
                "sort": "asc"
            }

            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error(f"Error fetching crypto bars for {symbol}: {e}")
            return {}

    def fetch_option_contracts(self, underlying_symbol: str):
        try:
            url = f"{self.base_url}/v2/options/contracts"
            params = {
                "underlying_symbols": underlying_symbol,
                "status": "active",
                "limit": 10000,
                "sort": "asc"
            }

            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error(f"Error fetching option contracts for {underlying_symbol}: {e}")
            return {}

    def fetch_option_snapshot(self, option_symbol: str):
        try:
            url = f"{self.base_url}/v2/options/snapshots"
            params = {
                "symbols": option_symbol,
                "limit": 1,
                "sort": "asc"
            }

            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error(f"Error fetching option snapshot for {option_symbol}: {e}")
            return {}

    # ---------------------------------------------------------
    # BRONZE INGESTION (RAW JSON STRING)
    # ---------------------------------------------------------

    def _write_raw_json_list(self, rows, table_name: str, mode: str):
        if not rows:
            logger.warning(f"No data to write for table {table_name}")
            return

        schema = StructType([StructField("json_raw", StringType(), True)])

        df = self.spark.createDataFrame(rows, schema=schema)
        df = df.withColumn("ingestion_timestamp", current_timestamp())

        df.write.format("delta").mode(mode).saveAsTable(table_name)
        logger.info(f"Wrote {len(rows)} rows to {table_name}")

    def ingest_stock_data(self, days_back: int = 90, mode: str = "append"):
        logger.info(f"Ingesting STOCK data (Bronze RAW JSON) for {days_back} days")

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        all_rows = []

        for symbol in self.top_stocks:
            raw = self.fetch_stock_bars(symbol, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
            if raw:
                all_rows.append({"json_raw": json.dumps(raw)})
                logger.info(f"Fetched RAW stock bars for {symbol}")

        self._write_raw_json_list(all_rows, "stock_bars_bronze", mode)

    def ingest_crypto_data(self, days_back: int = 90, mode: str = "append"):
        logger.info(f"Ingesting CRYPTO data (Bronze RAW JSON) for {days_back} days")

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        all_rows = []

        for symbol in self.top_crypto:
            raw = self.fetch_crypto_bars(symbol, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
            if raw:
                all_rows.append({"json_raw": json.dumps(raw)})
                logger.info(f"Fetched RAW crypto bars for {symbol}")

        self._write_raw_json_list(all_rows, "crypto_bars_bronze", mode)

    def ingest_options_data(self, mode: str = "append"):
        logger.info("Ingesting OPTIONS data (Bronze RAW JSON)")

        all_contracts_rows = []
        all_snapshots_rows = []

        for symbol in self.top_stocks:
            raw_contracts = self.fetch_option_contracts(symbol)
            if raw_contracts:
                all_contracts_rows.append({"json_raw": json.dumps(raw_contracts)})
                logger.info(f"Fetched RAW option contracts for {symbol}")

            contracts_list = raw_contracts.get("contracts", []) if isinstance(raw_contracts, dict) else []

            for contract in contracts_list:
                option_symbol = contract.get("symbol")
                if option_symbol:
                    raw_snapshot = self.fetch_option_snapshot(option_symbol)
                    if raw_snapshot:
                        all_snapshots_rows.append({"json_raw": json.dumps(raw_snapshot)})
                        logger.info(f"Fetched RAW option snapshot for {option_symbol}")

        self._write_raw_json_list(all_contracts_rows, "option_contracts_bronze", mode)
        self._write_raw_json_list(all_snapshots_rows, "option_snapshots_bronze", mode)

    # ---------------------------------------------------------
    # ORCHESTRATION
    # ---------------------------------------------------------

    def run_batch_ingestion(self, days_back: int = 90):
        logger.info("Starting BATCH ingestion (Bronze RAW JSON)")
        self.ingest_stock_data(days_back, mode="overwrite")
        self.ingest_crypto_data(days_back, mode="overwrite")
        self.ingest_options_data(mode="overwrite")
        logger.info("Batch ingestion completed")

    def run_incremental_ingestion(self, days_back: int = 1):
        logger.info("Starting INCREMENTAL ingestion (Bronze RAW JSON)")
        self.ingest_stock_data(days_back, mode="append")
        self.ingest_crypto_data(days_back, mode="append")
        self.ingest_options_data(mode="append")
        logger.info("Incremental ingestion completed")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    api_key = "PKDWTQFZRCSJEUQT4RUAT5BZCB"
    secret_key = "6GiiRCZyN6dVS63xBASsJsngagL164eewBAMcJUnQKiJ"
    ingestion_type = "batch"
    days_back = 90

    pipeline = SimpleAlpacaPipeline(api_key, secret_key)

    if ingestion_type == "batch":
        pipeline.run_batch_ingestion(days_back)
    else:
        pipeline.run_incremental_ingestion(days_back)

    logger.info("Pipeline execution completed successfully")


if __name__ == "__main__":
    main()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
