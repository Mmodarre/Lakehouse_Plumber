"""Market news/sentiment API custom DataSource for Spark Structured Streaming."""

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import requests
from datetime import datetime


class MarketNewsSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "market_news"

    def schema(self) -> StructType:
        return StructType([
            StructField("article_id", StringType(), False),
            StructField("ticker", StringType(), False),
            StructField("headline", StringType(), True),
            StructField("source", StringType(), True),
            StructField("published_at", TimestampType(), True),
            StructField("sentiment_score", DoubleType(), True),
            StructField("sentiment_label", StringType(), True),
            StructField("url", StringType(), True),
            StructField("fetched_at", TimestampType(), False),
        ])

    def streamReader(self, schema: StructType) -> "MarketNewsStreamReader":
        return MarketNewsStreamReader(
            api_key=self.options.get("api_key", ""),
            api_url=self.options.get("api_url", "https://api.marketaux.com/v1/news/all"),
            tickers=self.options.get("tickers", "AAPL,MSFT,GOOG"),
        )


class MarketNewsStreamReader(DataSourceStreamReader):
    def __init__(self, api_key: str, api_url: str, tickers: str):
        self._api_key = api_key
        self._api_url = api_url
        self._tickers = tickers
        self._offset = 0

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def latestOffset(self) -> dict:
        self._offset += 1
        return {"offset": self._offset}

    def partitions(self, start: dict, end: dict) -> list[InputPartition]:
        return [InputPartition(0)]

    def read(self, partition: InputPartition) -> list[tuple]:
        fetched_at = datetime.now()
        params = {
            "api_token": self._api_key,
            "symbols": self._tickers,
            "limit": 50,
        }

        try:
            response = requests.get(self._api_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception:
            return []

        rows = []
        for article in data.get("data", []):
            published = article.get("published_at")
            published_ts = (
                datetime.fromisoformat(published.replace("Z", "+00:00"))
                if published
                else None
            )
            sentiment = article.get("entities", [{}])[0] if article.get("entities") else {}
            rows.append((
                article.get("uuid", ""),
                self._tickers.split(",")[0],
                article.get("title", ""),
                article.get("source", ""),
                published_ts,
                float(sentiment.get("sentiment_score", 0.0)),
                sentiment.get("highlights", [{}])[0].get("sentiment", "neutral")
                if sentiment.get("highlights")
                else "neutral",
                article.get("url", ""),
                fetched_at,
            ))
        return rows
