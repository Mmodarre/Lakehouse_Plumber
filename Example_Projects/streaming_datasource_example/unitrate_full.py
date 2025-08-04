import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Real API-powered streaming data source for triggered mode
from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from typing import Iterator, Tuple
import json
import time
import requests
import os

# Simple working approach like example_progress.py
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
workspace_url = 'adb-984752964297111.11.azuredatabricks.net'


class CurrencyInputPartition(InputPartition):
    """Input partition for currency API data source"""
    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time

class CurrencyAPIStreamingDataSource(DataSource):
    """
    Real currency exchange data source powered by UniRateAPI.
    Fetches live exchange rates on each triggered pipeline run.
    """

    @classmethod
    def name(cls):
        return "currency_api_stream"

    def schema(self):
        return """
            base_currency string,
            target_currency string,
            exchange_rate double,
            api_timestamp timestamp,
            fetch_timestamp timestamp,
            rate_change_1h double,
            is_crypto boolean,
            data_source string,
            pipeline_run_id string
        """

    def streamReader(self, schema: StructType):
        return CurrencyAPIStreamingReader(schema, self.options)

class CurrencyAPIStreamingReader(DataSourceStreamReader):
    """Streaming reader that fetches real currency data from UniRateAPI with progress tracking"""
    
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
        # Your UniRateAPI key
        self.api_key = options.get("apiKey")
        # Base currencies to fetch
        self.base_currencies = options.get("baseCurrencies", "USD").split(",")
        
        # Progress tracking setup
        self.progress_path = options.get("progressPath")
        self.min_call_interval_seconds = int(options.get("minCallIntervalSeconds", "300"))  # 5 minutes default
        self.workspace_url = options.get("workspaceUrl")
        
        # Simple UC Volume setup like example_progress.py
        self.token = options.get("accessToken")
        self.progress_file_url = f"https://{workspace_url}/api/2.0/fs/files{self.progress_path}progress.json"
        
        # Load existing progress
        self._load_progress()
        
    def _load_progress(self):
        """Load progress from UC Volume"""
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        response = requests.get(self.progress_file_url, headers=headers)
        
        if response.status_code == 200:
            progress_data = response.json()
            self.last_api_call_time = progress_data.get('last_api_call_time', 0)
            self.total_api_calls = progress_data.get('total_api_calls', 0)
            print(f"✅ Loaded progress: last call at {self.last_api_call_time}, total calls: {self.total_api_calls}")
        else:
            # File doesn't exist yet, start fresh
            self.last_api_call_time = 0
            self.total_api_calls = 0
            print("📝 No existing progress found, starting fresh")
    
    def _save_progress(self):
        """Save progress to UC Volume"""
        url = f"{self.progress_file_url}?overwrite=true"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        progress_data = {
            "last_api_call_time": self.last_api_call_time,
            "total_api_calls": self.total_api_calls,
            "last_updated": int(time.time()),
            "base_currencies": self.base_currencies,
            "min_call_interval_seconds": self.min_call_interval_seconds
        }
        
        data = json.dumps(progress_data)
        response = requests.put(url, headers=headers, data=data)
    
    def _should_make_api_call(self):
        """Determine if enough time has passed to make a new API call"""
        current_time = int(time.time())
        time_since_last_call = current_time - self.last_api_call_time
        
        should_call = time_since_last_call >= self.min_call_interval_seconds
        
        if not should_call:
            remaining_time = self.min_call_interval_seconds - time_since_last_call
            print(f"⏳ Skipping API call - {remaining_time} seconds remaining until next allowed call")
        
        return should_call
        
    def initialOffset(self) -> dict:
        """Returns the initial start offset using loaded progress"""
        import time
        return {"fetch_time": int(time.time() * 1000), "should_fetch": self._should_make_api_call()}

    def latestOffset(self) -> dict:
        """Returns latest offset - advances only if should fetch"""
        import time
        current_time_ms = int(time.time() * 1000)
        should_fetch = self._should_make_api_call()
        return {"fetch_time": current_time_ms, "should_fetch": should_fetch}

    def partitions(self, start: dict, end: dict):
        """Plans the partitioning of the current microbatch"""
        return [CurrencyInputPartition(start.get("fetch_time", 0), end.get("fetch_time", 0))]

    def commit(self, end: dict):
        """Called when the query has finished processing data - save progress if API was called"""
        if end.get("should_fetch", False):
            self._save_progress()

    def read(self, partition) -> Iterator[Tuple]:
        """Fetches real currency exchange data from UniRateAPI with rate limiting"""
        from datetime import datetime
        import builtins
        
        # Check if we should actually make API calls
        if not self._should_make_api_call():
            print("⏳ Skipping API call due to rate limiting")
            return iter([])  # Return empty iterator
        
        # Update progress tracking
        self.last_api_call_time = int(time.time())
        self.total_api_calls += 1
        
        # Generate unique run ID for this pipeline execution
        run_id = f"run_{self.total_api_calls}_{int(time.time())}"
        fetch_timestamp = datetime.now()
        
        print(f"🌐 Making API call #{self.total_api_calls} at {fetch_timestamp}")
        
        # Fetch data for each base currency
        for base_currency in self.base_currencies:
            try:
                # Call UniRateAPI to get current exchange rates
                api_url = f"https://api.unirateapi.com/api/rates"
                params = {
                    "api_key": self.api_key,
                    "from": base_currency
                }
                
                response = requests.get(api_url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Extract rates from the API response
                    if "rates" in data:
                        rates = data["rates"]
                        api_timestamp = datetime.now()  # UniRate doesn't provide exact timestamp
                        
                        # Common target currencies to focus on
                        target_currencies = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "BTC", "ETH"]
                        
                        for target_currency in target_currencies:
                            if target_currency in rates and target_currency != base_currency:
                                rate = float(rates[target_currency])
                                
                                # Determine if this is a cryptocurrency
                                is_crypto = target_currency in ["BTC", "ETH", "LTC", "XRP", "ADA"]
                                
                                # Simulate rate change (in real implementation, you'd store previous values)
                                # For demo, we'll calculate a small random change
                                import random
                                rate_change_1h = builtins.round(random.uniform(-0.05, 0.05), 4)
                                
                                yield (
                                    base_currency,
                                    target_currency,
                                    rate,
                                    api_timestamp,
                                    fetch_timestamp,
                                    rate_change_1h,
                                    is_crypto,
                                    "UniRateAPI",
                                    run_id
                                )
                
                # Small delay between API calls to be respectful
                time.sleep(0.5)
                        
            except Exception as e:
                print(f"Error fetching data for {base_currency}: {str(e)}")
                # Yield an error record for monitoring
                yield (
                    base_currency,
                    "ERROR",
                    0.0,
                    datetime.now(),
                    fetch_timestamp,
                    0.0,
                    False,
                    f"ERROR: {str(e)}",
                    run_id
                )

# Register the data source
try:
    spark.dataSource.register(CurrencyAPIStreamingDataSource)
    print("✅ Currency API streaming data source registered successfully!")
except Exception as e:
    print(f"⚠️  Warning: Could not register data source during module load: {e}")

# Bronze Layer - Raw currency data from API with Progress Tracking
@dlt.table(
    comment="Raw currency exchange rates from UniRateAPI with UC Volume progress tracking",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def currency_exchange_bronze():
    """
    Bronze table that fetches live currency exchange rates from UniRateAPI.
    Features UC Volume-based progress tracking to prevent excessive API calls.
    Perfect for demonstrating real API integration in triggered mode with cost optimization.
    """
    
    # Try to register data source if not already registered
    try:
        spark.dataSource.register(CurrencyAPIStreamingDataSource)
    except Exception:
        pass  # Ignore if already registered
    
    return (
        spark.readStream
        .format("currency_api_stream")
        .option("apiKey", "zDn5ypTHgrpvE57j0n2BYY6nobCclA3t5KHGBEDoZHouYplTLktcXgnso8OzBMkp")
        .option("baseCurrencies", "USD,EUR,GBP")  # Currencies to fetch rates for
        # 🔄 Progress Tracking Options (UC Volume-based)
        .option("progressPath", "/Volumes/mehdidatalake_catalog/unirate/checkpoints/")  # UC Volume path - CONFIGURABLE
        .option("minCallIntervalSeconds", "300")  # 5 minutes between API calls - CONFIGURABLE
        # 🌐 Optional Workspace Configuration (auto-detected in most DLT environments)
        # .option("workspaceUrl", "your-workspace.cloud.databricks.com")  # Override auto-detection if needed
        # .option("accessToken", "your-access-token")  # Better to use DATABRICKS_TOKEN env var
        .load()
        .select(
            col("base_currency"),
            col("target_currency"),
            col("exchange_rate"),
            col("api_timestamp"),
            col("fetch_timestamp"),
            col("rate_change_1h"),
            col("is_crypto"),
            col("data_source"),
            col("pipeline_run_id"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )

# Silver Layer - Cleaned and enriched currency data
@dlt.table(
    comment="Cleaned currency exchange rates with market insights",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_exchange_rate", "exchange_rate > 0")
@dlt.expect_or_drop("valid_currencies", "base_currency != 'ERROR' AND target_currency != 'ERROR'")
@dlt.expect_or_drop("realistic_rate", "exchange_rate < 1000000")  # Sanity check
def currency_exchange_silver():
    """
    Silver table with cleaned currency data and derived market insights.
    """
    return (
        spark.readStream.table("currency_exchange_bronze")
        .filter(col("target_currency") != "ERROR")  # Filter out error records
        .select(
            col("base_currency"),
            col("target_currency"),
            col("exchange_rate"),
            col("api_timestamp"),
            col("fetch_timestamp"),
            col("rate_change_1h"),
            col("is_crypto"),
            col("data_source"),
            col("pipeline_run_id"),
            col("ingestion_timestamp"),
            
            # Create currency pair in standard format
            concat(col("base_currency"), lit("/"), col("target_currency")).alias("currency_pair"),
            
            # Categorize exchange rate magnitude
            when(col("exchange_rate") >= 100, "HIGH_VALUE")
            .when(col("exchange_rate") >= 1, "NORMAL_VALUE")
            .when(col("exchange_rate") >= 0.01, "LOW_VALUE")
            .otherwise("MICRO_VALUE").alias("rate_category"),
            
            # Categorize rate change
            when(abs(col("rate_change_1h")) >= 0.02, "HIGH_VOLATILITY")
            .when(abs(col("rate_change_1h")) >= 0.01, "MEDIUM_VOLATILITY")
            .otherwise("LOW_VOLATILITY").alias("volatility_level"),
            
            # Market type classification
            when(col("is_crypto") == True, "CRYPTOCURRENCY")
            .when(col("base_currency").isin("USD", "EUR", "GBP", "JPY"), "MAJOR_FIAT")
            .otherwise("MINOR_FIAT").alias("market_type"),
            
            # Calculate inverse rate for reference
            (1.0 / col("exchange_rate")).alias("inverse_rate"),
            
            # Time-based features
            date_format(col("fetch_timestamp"), "yyyy-MM-dd HH:mm").alias("fetch_time_rounded"),
            hour(col("fetch_timestamp")).alias("fetch_hour"),
            dayofweek(col("fetch_timestamp")).alias("fetch_day_of_week")
        )
    )

# Gold Layer - Currency market dashboard data
@dlt.table(
    comment="Currency market dashboard with aggregated insights per pipeline run",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def currency_market_dashboard():
    """
    Gold table with aggregated currency market insights per pipeline run.
    Perfect for monitoring API data freshness and market trends.
    """
    return (
        spark.readStream.table("currency_exchange_silver")
        .groupBy(
            col("pipeline_run_id"),
            col("fetch_time_rounded"),
            col("base_currency"),
            col("market_type")
        )
        .agg(
            count("*").alias("currency_pairs_count"),
            avg("exchange_rate").alias("avg_exchange_rate"),
            max("exchange_rate").alias("max_exchange_rate"),
            min("exchange_rate").alias("min_exchange_rate"),
            avg("rate_change_1h").alias("avg_rate_change"),
            sum(when(col("volatility_level") == "HIGH_VOLATILITY", 1).otherwise(0)).alias("high_volatility_pairs"),
            sum(when(col("is_crypto") == True, 1).otherwise(0)).alias("crypto_pairs_count"),
            collect_list("currency_pair").alias("currency_pairs_list")
        )
        .select(
            col("pipeline_run_id"),
            col("fetch_time_rounded"),
            col("base_currency"),
            col("market_type"),
            col("currency_pairs_count"),
            round(col("avg_exchange_rate"), 6).alias("avg_exchange_rate"),
            col("max_exchange_rate"),
            col("min_exchange_rate"),
            round(col("avg_rate_change"), 4).alias("avg_rate_change"),
            col("high_volatility_pairs"),
            col("crypto_pairs_count"),
            
            # Market sentiment indicators
            when(col("avg_rate_change") > 0.005, "BULLISH")
            .when(col("avg_rate_change") < -0.005, "BEARISH")
            .otherwise("NEUTRAL").alias("market_sentiment"),
            
            # Data freshness indicator
            when(col("currency_pairs_count") >= 8, "COMPLETE")
            .when(col("currency_pairs_count") >= 5, "PARTIAL")
            .otherwise("LIMITED").alias("data_completeness"),
            
            current_timestamp().alias("dashboard_updated_at")
        )
    )

# Gold Layer - Real-time currency alerts
@dlt.table(
    comment="Real-time alerts for significant currency movements",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def currency_movement_alerts():
    """
    Gold table for real-time alerts on significant currency movements.
    Filters for high volatility and unusual rate changes.
    """
    return (
        spark.readStream.table("currency_exchange_silver")
        .filter(
            (col("volatility_level") == "HIGH_VOLATILITY") |
            (col("rate_category") == "MICRO_VALUE") |  # Very low rates might indicate issues
            (abs(col("rate_change_1h")) > 0.03)  # >3% change
        )
        .select(
            col("currency_pair"),
            col("base_currency"),
            col("target_currency"),
            col("exchange_rate"),
            col("rate_change_1h"),
            col("volatility_level"),
            col("market_type"),
            col("pipeline_run_id"),
            col("fetch_timestamp"),
            
            # Alert categorization
            when(abs(col("rate_change_1h")) > 0.05, "EXTREME_MOVEMENT")
            .when(col("volatility_level") == "HIGH_VOLATILITY", "HIGH_VOLATILITY_ALERT")
            .when(col("rate_category") == "MICRO_VALUE", "UNUSUAL_RATE_ALERT")
            .otherwise("STANDARD_ALERT").alias("alert_type"),
            
            # Alert priority
            when(col("market_type") == "MAJOR_FIAT", "HIGH")
            .when(col("market_type") == "CRYPTOCURRENCY", "MEDIUM")
            .otherwise("LOW").alias("alert_priority"),
            
            current_timestamp().alias("alert_timestamp")
        )
    )

# Reporting Layer - Materialized View aggregates for BI dashboards
@dlt.table(
    comment="Materialized View currency market reporting aggregates for BI dashboards",
    table_properties={
        "quality": "reporting",
        "pipelines.autoOptimize.managed": "true"
    }
)
def currency_market_report():
    """
    Materialized View reporting table that aggregates historical currency data.
    Perfect for BI dashboards, trend analysis, and executive reporting.
    Reads from silver layer as batch data rather than streaming.
    """
    return (
        spark.read.table("currency_exchange_silver")  # Materialized View read
        .groupBy(
            col("base_currency"),
            col("target_currency"), 
            col("currency_pair"),
            col("market_type"),
            date_format(col("fetch_timestamp"), "yyyy-MM-dd").alias("report_date")
        )
        .agg(
            count("*").alias("total_observations"),
            avg("exchange_rate").alias("avg_daily_rate"),
            min("exchange_rate").alias("daily_low"),
            max("exchange_rate").alias("daily_high"),
            first("exchange_rate").alias("opening_rate"),
            last("exchange_rate").alias("closing_rate"),
            avg("rate_change_1h").alias("avg_volatility"),
            stddev("exchange_rate").alias("rate_volatility"),
            
            # Calculate daily price movement
            ((last("exchange_rate") - first("exchange_rate")) / first("exchange_rate") * 100).alias("daily_change_percent"),
            
            # Count different volatility levels
            sum(when(col("volatility_level") == "HIGH_VOLATILITY", 1).otherwise(0)).alias("high_volatility_periods"),
            sum(when(col("volatility_level") == "MEDIUM_VOLATILITY", 1).otherwise(0)).alias("medium_volatility_periods"),
            sum(when(col("volatility_level") == "LOW_VOLATILITY", 1).otherwise(0)).alias("low_volatility_periods"),
            
            # Time-based aggregations
            min("fetch_timestamp").alias("first_observation"),
            max("fetch_timestamp").alias("last_observation"),
            countDistinct("pipeline_run_id").alias("pipeline_runs_count")
        )
        .select(
            col("report_date"),
            col("base_currency"),
            col("target_currency"),
            col("currency_pair"),
            col("market_type"),
            col("total_observations"),
            
            # Rate statistics
            round(col("avg_daily_rate"), 6).alias("avg_daily_rate"),
            col("daily_low"),
            col("daily_high"),
            col("opening_rate"),
            col("closing_rate"),
            round(col("daily_change_percent"), 4).alias("daily_change_percent"),
            round(col("rate_volatility"), 6).alias("rate_volatility"),
            
            # Trading range analysis
            round(((col("daily_high") - col("daily_low")) / col("avg_daily_rate") * 100), 4).alias("daily_range_percent"),
            
            # Volatility breakdown
            col("high_volatility_periods"),
            col("medium_volatility_periods"), 
            col("low_volatility_periods"),
            round((col("high_volatility_periods") / col("total_observations") * 100), 2).alias("high_volatility_percentage"),
            
            # Performance indicators
            when(col("daily_change_percent") > 1, "STRONG_GAIN")
            .when(col("daily_change_percent") > 0.1, "GAIN")
            .when(col("daily_change_percent") < -1, "STRONG_LOSS")
            .when(col("daily_change_percent") < -0.1, "LOSS")
            .otherwise("STABLE").alias("daily_performance"),
            
            # Risk assessment
            when(col("rate_volatility") > 0.01, "HIGH_RISK")
            .when(col("rate_volatility") > 0.005, "MEDIUM_RISK")
            .otherwise("LOW_RISK").alias("risk_level"),
            
            # Data quality indicators
            col("pipeline_runs_count"),
            col("first_observation"),
            col("last_observation"),
            
            current_timestamp().alias("report_generated_at")
        )
    )

@dlt.table(
    comment="Weekly currency market trends for executive reporting",
    table_properties={
        "quality": "reporting",
        "pipelines.autoOptimize.managed": "true"
    }
)
def currency_weekly_trends():
    """
    Weekly aggregated trends for executive dashboards and strategic planning.
    Non-streaming table that provides week-over-week analysis.
    """
    return (
        spark.read.table("currency_market_report")  # Read from daily report
        .withColumn("report_year", year(col("report_date")))
        .withColumn("report_week_number", weekofyear(col("report_date")))
        .withColumn("report_week", concat(
            col("report_year").cast("string"), 
            lit("-W"), 
            lpad(col("report_week_number").cast("string"), 2, "0")
        ))
        .groupBy(
            col("report_week"),
            col("base_currency"),
            col("target_currency"),
            col("currency_pair"),
            col("market_type")
        )
        .agg(
            count("*").alias("trading_days"),
            sum("total_observations").alias("total_weekly_observations"),
            avg("avg_daily_rate").alias("avg_weekly_rate"),
            min("daily_low").alias("weekly_low"),
            max("daily_high").alias("weekly_high"),
            first("opening_rate").alias("week_opening_rate"),
            last("closing_rate").alias("week_closing_rate"),
            avg("daily_change_percent").alias("avg_daily_change"),
            sum("daily_change_percent").alias("cumulative_weekly_change"),
            avg("rate_volatility").alias("avg_weekly_volatility"),
            
            # Weekly performance metrics
            sum("high_volatility_periods").alias("total_high_volatility_periods"),
            avg("high_volatility_percentage").alias("avg_volatility_percentage")
        )
        .select(
            col("report_week"),
            col("base_currency"),
            col("target_currency"),
            col("currency_pair"),
            col("market_type"),
            col("trading_days"),
            col("total_weekly_observations"),
            
            # Weekly rate analysis
            round(col("avg_weekly_rate"), 6).alias("avg_weekly_rate"),
            col("weekly_low"),
            col("weekly_high"),
            col("week_opening_rate"),
            col("week_closing_rate"),
            
            # Weekly performance
            round(col("cumulative_weekly_change"), 4).alias("weekly_change_percent"),
            round(((col("week_closing_rate") - col("week_opening_rate")) / col("week_opening_rate") * 100), 4).alias("week_over_week_change"),
            round(col("avg_weekly_volatility"), 6).alias("avg_weekly_volatility"),
            
            # Trading range
            round(((col("weekly_high") - col("weekly_low")) / col("avg_weekly_rate") * 100), 4).alias("weekly_range_percent"),
            
            # Weekly trend classification
            when(col("cumulative_weekly_change") > 2, "STRONG_WEEKLY_UPTREND")
            .when(col("cumulative_weekly_change") > 0.5, "WEEKLY_UPTREND")
            .when(col("cumulative_weekly_change") < -2, "STRONG_WEEKLY_DOWNTREND")
            .when(col("cumulative_weekly_change") < -0.5, "WEEKLY_DOWNTREND")
            .otherwise("WEEKLY_SIDEWAYS").alias("weekly_trend"),
            
            # Market activity level
            when(col("avg_volatility_percentage") > 20, "HIGHLY_ACTIVE")
            .when(col("avg_volatility_percentage") > 10, "ACTIVE")
            .otherwise("CALM").alias("market_activity"),
            
            current_timestamp().alias("report_generated_at")
        )
    )

@dlt.table(
    comment="Currency pair correlation matrix for portfolio analysis",
    table_properties={
        "quality": "reporting",
        "pipelines.autoOptimize.managed": "true"
    }
)
def currency_correlation_matrix():
    """
    Non-streaming correlation analysis between currency pairs for risk management.
    Useful for portfolio diversification and hedging strategies.
    """
    return (
        # Self-join to create pair combinations for correlation analysis
        spark.read.table("currency_market_report")
        .alias("a")
        .join(
            spark.read.table("currency_market_report").alias("b"),
            (col("a.report_date") == col("b.report_date")) & 
            (col("a.currency_pair") < col("b.currency_pair"))  # Avoid duplicate pairs
        )
        .groupBy(
            col("a.currency_pair").alias("currency_pair_1"),
            col("b.currency_pair").alias("currency_pair_2"),
            col("a.market_type").alias("market_type_1"),
            col("b.market_type").alias("market_type_2")
        )
        .agg(
            count("*").alias("observation_days"),
            corr(col("a.daily_change_percent"), col("b.daily_change_percent")).alias("price_correlation"),
            corr(col("a.rate_volatility"), col("b.rate_volatility")).alias("volatility_correlation"),
            avg(col("a.daily_change_percent")).alias("avg_change_pair_1"),
            avg(col("b.daily_change_percent")).alias("avg_change_pair_2"),
            stddev(col("a.daily_change_percent")).alias("volatility_pair_1"),
            stddev(col("b.daily_change_percent")).alias("volatility_pair_2")
        )
        .filter(col("observation_days") >= 5)  # Minimum observations for meaningful correlation
        .select(
            col("currency_pair_1"),
            col("currency_pair_2"),
            col("market_type_1"),
            col("market_type_2"),
            col("observation_days"),
            
            # Correlation metrics
            round(col("price_correlation"), 4).alias("price_correlation"),
            round(col("volatility_correlation"), 4).alias("volatility_correlation"),
            
            # Pair performance comparison
            round(col("avg_change_pair_1"), 4).alias("avg_change_pair_1"),
            round(col("avg_change_pair_2"), 4).alias("avg_change_pair_2"),
            round(col("volatility_pair_1"), 4).alias("volatility_pair_1"),
            round(col("volatility_pair_2"), 4).alias("volatility_pair_2"),
            
            # Correlation strength classification
            when(abs(col("price_correlation")) > 0.8, "STRONG_CORRELATION")
            .when(abs(col("price_correlation")) > 0.5, "MODERATE_CORRELATION")
            .when(abs(col("price_correlation")) > 0.2, "WEAK_CORRELATION")
            .otherwise("NO_CORRELATION").alias("correlation_strength"),
            
            # Correlation direction
            when(col("price_correlation") > 0.1, "POSITIVE")
            .when(col("price_correlation") < -0.1, "NEGATIVE")
            .otherwise("NEUTRAL").alias("correlation_direction"),
            
            # Diversification benefit
            when((abs(col("price_correlation")) < 0.3) & (col("market_type_1") != col("market_type_2")), "HIGH_DIVERSIFICATION")
            .when(abs(col("price_correlation")) < 0.5, "MEDIUM_DIVERSIFICATION")
            .otherwise("LOW_DIVERSIFICATION").alias("diversification_benefit"),
            
            current_timestamp().alias("analysis_generated_at")
        )
    ) 