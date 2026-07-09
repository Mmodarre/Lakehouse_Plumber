"""GPG-decrypt a binary Kafka value column and parse the resulting JSON payload.

The GPG private key and its passphrase are injected via Databricks secrets
(resolved by LHP as ${secret:gpg/private_key} and ${secret:gpg/passphrase}).
Both arrive as plain strings in the `parameters` dict at runtime.

Requires: pgpy (add to cluster/requirements if not already present).
"""

import json

import pgpy
from pgpy.constants import CompressionAlgorithm, HashAlgorithm, SymmetricKeyAlgorithm
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType


def _make_decrypt_udf(private_key_armored: str, passphrase: str):
    """Return a UDF that decrypts one ciphertext bytes value → JSON string."""

    def _decrypt(ciphertext: bytes) -> str | None:
        if ciphertext is None:
            return None
        try:
            key, _ = pgpy.PGPKey.from_blob(private_key_armored)
            with key.unlock(passphrase):
                msg = pgpy.PGPMessage.from_blob(ciphertext)
                return str(msg.decrypt(key).message)
        except Exception:
            return None

    return F.udf(_decrypt, StringType())


def decrypt_and_parse(
    df: DataFrame, spark: SparkSession, parameters: dict
) -> DataFrame:
    """Decrypt the binary `value` column and explode the JSON fields.

    Expected parameters (injected by LHP from YAML):
      private_key  – armored GPG private key string
      passphrase   – key passphrase
      json_schema  – optional comma-separated field list; when absent all fields
                     are returned as a map<string,string>
    """
    private_key: str = parameters["private_key"]
    passphrase: str = parameters["passphrase"]
    json_schema: str | None = parameters.get("json_schema")

    decrypt_udf = _make_decrypt_udf(private_key, passphrase)

    # Decrypt binary value → JSON string; keep Kafka metadata columns.
    df = df.withColumn("_decrypted_json", decrypt_udf(F.col("value")))

    if json_schema:
        # Caller supplied an explicit schema string, e.g.:
        # "id STRING, event_type STRING, occurred_at TIMESTAMP, payload STRING"
        df = df.withColumn(
            "parsed", F.from_json(F.col("_decrypted_json"), json_schema)
        ).select(
            F.col("key").cast(StringType()).alias("event_key"),
            "parsed.*",
            F.col("topic").alias("source_topic"),
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            "_processing_timestamp",
        )
    else:
        # No schema known at design time – return fields as map<string,string>.
        df = df.withColumn(
            "parsed",
            F.from_json(F.col("_decrypted_json"), MapType(StringType(), StringType())),
        ).select(
            F.col("key").cast(StringType()).alias("event_key"),
            "parsed",
            F.col("topic").alias("source_topic"),
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            "_processing_timestamp",
        )

    return df.drop("_decrypted_json")
