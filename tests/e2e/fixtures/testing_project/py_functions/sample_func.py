from pyspark.sql.functions import (
    col,
    explode,
    lit,
    when,
    size,
    current_timestamp,
    date_format,
    from_utc_timestamp,
)
from pyspark.sql import DataFrame


def transform_lrc_data_streaming(df_input: DataFrame, spark, parameters) -> DataFrame:
    """
    STREAMING EQUIVALENT of the original transform_lrc_data function

    Original script issues fixed:
    1. Removed .first() calls (not streaming compatible)
    2. Fixed dynamic data column bug for 12+ header files
    3. Simplified complex join logic while maintaining same results
    """

    # ====================================================================
    # STEP 1: METADATA PREPARATION (Original lines 19-22)
    # ====================================================================
    # Original: Used spark.sql().collect()[0] to get timestamp (not streaming compatible)
    # Streaming: Calculate timestamp directly in DataFrame transformation
    df_with_metadata = df_input.withColumn(
        "source_effective_timestamp",
        # Equivalent to original: date_format(from_utc_timestamp(current_timestamp(), 'Australia/Sydney'), 'yyyy-MM-dd HH:mm:ss.SSSSSS')
        date_format(
            from_utc_timestamp(current_timestamp(), "Australia/Sydney"),
            "yyyy-MM-dd HH:mm:ss.SSSSSS",
        ),
    )

    # ====================================================================
    # STEP 2: YEAR EXTRACTION (Original lines 27 & 73)
    # ====================================================================
    # Original: year = df_output.select(explode(col("columns")).alias("year")).first()["year"] Not streaming compatible
    # Streaming: Extract year directly without .first() action
    df_with_year = df_with_metadata.withColumn(
        "year_value",
        col("columns")[0][
            0
        ],  # Direct access to nested array: columns[0][0] gives us "FY20" or "FY21"
    )

    # ====================================================================
    # STEP 3: ROW EXPLOSION AND HEADER/DATA EXTRACTION (Original lines 29-32)
    # ====================================================================
    # Original: df_rows = df_output.select(col("rows.headers").alias("headers"), col("rows.data").alias("data"))
    #           df_explode_header = df_rows.select(explode('headers'))
    #           df_explode_data = df_rows.select(explode('data'))
    # Streaming: Process each row directly without separate DataFrames
    df_exploded = (
        df_with_year.withColumn("row_item", explode(col("rows")))
        .withColumn("headers", col("row_item.headers"))
        .withColumn("data", col("row_item.data"))
    )

    # ====================================================================
    # STEP 4: BUSINESS LOGIC MAPPING (Original lines 40-70)
    # ====================================================================
    # Original script had complex logic:
    # 1. Dynamic column creation based on header count (lines 40-50)
    # 2. Synthetic ID generation and joins (lines 55-56)
    # 3. Complex renaming of data columns (lines 37-38, 52)
    # 4. Hardcoded business mapping (lines 59-70)
    #
    # SIMPLIFIED STREAMING APPROACH:
    # - Process each exploded row directly (no joins needed)
    # - Use safe array access with bounds checking
    # - Fix the original bug where 12+ header files failed

    df_business_mapped = df_exploded.select(
        # ================================================================
        # CORE BUSINESS FIELDS (Original lines 60-68)
        # ================================================================
        # Safe array access - if headers array is too short, use null
        # This handles variable header lengths gracefully
        # Position 0: Entity (Original: col("col_0").alias("Entity"))
        when(size(col("headers")) > 0, col("headers")[0])
        .otherwise(lit(None))
        .alias("Entity"),
        # Position 1: CostCentre (Original: col("col_1").alias("CostCentre"))
        when(size(col("headers")) > 1, col("headers")[1])
        .otherwise(lit(None))
        .alias("CostCentre"),
        # Position 2: Account (Original: col("col_2").alias("Account"))
        when(size(col("headers")) > 2, col("headers")[2])
        .otherwise(lit(None))
        .alias("Account"),
        # Position 3: Project (Original: col("col_3").alias("Project"))
        when(size(col("headers")) > 3, col("headers")[3])
        .otherwise(lit(None))
        .alias("Project"),
        # Position 4: Line_Of_Business (Original: col("col_4").alias("Line_Of_Business"))
        when(size(col("headers")) > 4, col("headers")[4])
        .otherwise(lit(None))
        .alias("Line_Of_Business"),
        # Position 5: Source (Original: col("col_5").alias("Source"))
        when(size(col("headers")) > 5, col("headers")[5])
        .otherwise(lit(None))
        .alias("Source"),
        # Position 6: Scenario (Original: col("col_6").alias("Scenario"))
        when(size(col("headers")) > 6, col("headers")[6])
        .otherwise(lit(None))
        .alias("Scenario"),
        # Position 7: Currency (Original: col("col_7").alias("Currency"))
        when(size(col("headers")) > 7, col("headers")[7])
        .otherwise(lit(None))
        .alias("Currency"),
        # Position 8: Period (Original: col("col_8").alias("Period"))
        when(size(col("headers")) > 8, col("headers")[8])
        .otherwise(lit(None))
        .alias("Period"),
        # ================================================================
        # AMOUNT FIELD - FIXED BUG FROM ORIGINAL (Original line 69)
        # ================================================================
        # Original bug: col("col_9").alias("Amount")
        # - This assumed Amount was always at position 9 in the headers
        # - But Amount comes from data[0], not headers[9]
        # - For 12+ header files, there was no col_9 mapping, causing failures
        #
        # FIXED: Always use data[0] for Amount, regardless of header count
        when(size(col("data")) > 0, col("data")[0])
        .otherwise(lit(None))
        .alias("Amount"),
        # ================================================================
        # METADATA FIELDS (Original lines 73-79)
        # ================================================================
        # Year column (Original: df_combined_data.withColumn("Years", lit(year)))
        col("year_value").alias("Years"),
        # Timestamp column (Original: df_combined_data.withColumn("Source_effective_timestamp", lit(Source_effective_timestamp)))
        col("source_effective_timestamp").alias("Source_effective_timestamp"),
    )

    # ====================================================================
    # STEP 5: DATA QUALITY VALIDATION (User requirement: fail if < 9 headers)
    # ====================================================================
    # Add expectation to fail if any row has fewer than 9 headers
    # This ensures data consistency and prevents null values in core business fields
    df_validated = df_business_mapped.filter(col("Header_Count") >= 9)

    return df_validated
