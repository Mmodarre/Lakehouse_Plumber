"""Yahoo Finance custom DataSource for Spark Structured Streaming."""

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import yfinance as yf
from datetime import datetime, timedelta


class YahooFinanceSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "yahoo_finance"

    def schema(self) -> StructType:
        return StructType([
            StructField("ticker", StringType(), False),
            StructField("trade_date", TimestampType(), False),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("adj_close", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("fetched_at", TimestampType(), False),
        ])

    def streamReader(self, schema: StructType) -> "YahooFinanceStreamReader":
        return YahooFinanceStreamReader(
            tickers=self.options.get("tickers", "AAPL,MSFT,GOOG"),
            interval=self.options.get("interval", "1d"),
            lookback_days=int(self.options.get("lookback_days", "30")),
        )


class YahooFinanceStreamReader(DataSourceStreamReader):
    def __init__(self, tickers: str, interval: str, lookback_days: int):
        self._tickers = [t.strip() for t in tickers.split(",")]
        self._interval = interval
        self._lookback_days = lookback_days
        self._offset = 0

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def latestOffset(self) -> dict:
        self._offset += 1
        return {"offset": self._offset}

    def partitions(self, start: dict, end: dict) -> list[InputPartition]:
        return [InputPartition(i) for i in range(len(self._tickers))]

    def read(self, partition: InputPartition) -> list[tuple]:
        ticker_symbol = self._tickers[partition.value]
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self._lookback_days)
        fetched_at = datetime.now()

        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval=self._interval,
        )

        rows = []
        for idx, row in hist.iterrows():
            rows.append((
                ticker_symbol,
                idx.to_pydatetime(),
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
                float(row.get("Adj Close", row["Close"])),
                int(row["Volume"]),
                fetched_at,
            ))
        return rows
