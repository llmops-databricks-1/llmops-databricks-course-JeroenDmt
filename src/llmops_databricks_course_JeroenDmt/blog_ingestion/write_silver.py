"""Create Silver tables for Databricks blog posts from Bronze."""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime

from bs4 import BeautifulSoup
from langchain_text_splitters import HTMLHeaderTextSplitter
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from llmops_databricks_course_JeroenDmt.blog_ingestion.write_bronze import (
    _bronze_table_full_name,
)
from llmops_databricks_course_JeroenDmt.config.models import ProjectConfig

SILVER_TABLE_NAME = "silver_blog_posts"
SILVER_CHUNKS_TABLE_NAME = "silver_blog_post_chunks"


def _silver_table_full_name(config: ProjectConfig) -> str:
    return f"{config.catalog}.{config.db_schema}.{SILVER_TABLE_NAME}"


def _silver_chunks_table_full_name(config: ProjectConfig) -> str:
    return f"{config.catalog}.{config.db_schema}.{SILVER_CHUNKS_TABLE_NAME}"


def _html_to_text(html: str | None) -> str | None:
    """Convert complex blog HTML into readable plain text using lxml parser."""
    if html is None:
        return None
    soup = BeautifulSoup(html, "lxml")
    for element in soup(["script", "style", "nav", "footer", "header", "aside"]):
        element.decompose()
    text = soup.get_text(separator=" ")
    return " ".join(text.split())


def _clean_html_for_chunking(html: str | None) -> str:
    """Remove noisy sections while preserving header/body tag structure."""
    if html is None:
        return ""

    soup = BeautifulSoup(html, "lxml")
    for element in soup(
        ["script", "style", "nav", "footer", "aside", "noscript", "form", "svg"]
    ):
        element.decompose()
    return str(soup)


def _extract_header(
    metadata: dict[str, str],
    key: str,
    fallback_key: str,
) -> str | None:
    value = metadata.get(key, metadata.get(fallback_key))
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned if cleaned else None


def _split_html_to_chunks(cleaned_html: str) -> list[dict[str, str | None]]:
    """Split HTML into chunks and attach hierarchical header metadata."""
    splitter = HTMLHeaderTextSplitter(
        headers_to_split_on=[("h1", "Header 1"), ("h2", "Header 2"), ("h3", "Header 3")]
    )
    docs = splitter.split_text(cleaned_html)

    chunks: list[dict[str, str | None]] = []
    for doc in docs:
        chunk_text = " ".join(doc.page_content.split())
        if not chunk_text:
            continue

        metadata: dict[str, str] = {
            str(k): str(v) for k, v in (doc.metadata or {}).items() if v is not None
        }
        header_h1 = _extract_header(metadata, "h1", "Header 1")
        header_h2 = _extract_header(metadata, "h2", "Header 2")
        header_h3 = _extract_header(metadata, "h3", "Header 3")

        headers = [h for h in [header_h1, header_h2, header_h3] if h]
        section_path = " > ".join(headers) if headers else None

        chunks.append(
            {
                "header_h1": header_h1,
                "header_h2": header_h2,
                "header_h3": header_h3,
                "section_path": section_path,
                "chunk_text": chunk_text,
            }
        )

    return chunks


def _build_chunk_rows(row: Row) -> list[dict[str, object]]:
    """Convert a Bronze row into chunk rows for Silver chunk table."""
    cleaned_html = _clean_html_for_chunking(row.raw_html)
    chunks = _split_html_to_chunks(cleaned_html)

    if not chunks:
        fallback_text = _html_to_text(row.raw_html)
        if fallback_text:
            chunks = [
                {
                    "header_h1": None,
                    "header_h2": None,
                    "header_h3": None,
                    "section_path": None,
                    "chunk_text": fallback_text,
                }
            ]

    now = datetime.now(UTC)
    chunk_rows: list[dict[str, object]] = []
    for idx, chunk in enumerate(chunks):
        text = str(chunk["chunk_text"]).strip()
        if not text:
            continue
        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        stable_id = hashlib.sha256(f"{row.url}|{idx}|{content_hash}".encode()).hexdigest()

        chunk_rows.append(
            {
                "chunk_id": str(uuid.UUID(stable_id[:32])),
                "url": row.url,
                "title": row.title,
                "published_at": row.published_at,
                "category": row.category,
                "tags": row.tags,
                "chunk_index": idx,
                "header_h1": chunk["header_h1"],
                "header_h2": chunk["header_h2"],
                "header_h3": chunk["header_h3"],
                "section_path": chunk["section_path"],
                "chunk_text": text,
                "chunk_char_count": len(text),
                "content_hash": content_hash,
                "ingested_at": now,
            }
        )

    return chunk_rows


def _ensure_silver_chunks_table(spark: SparkSession, config: ProjectConfig) -> None:
    table_name = _silver_chunks_table_full_name(config)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            chunk_id STRING NOT NULL,
            url STRING NOT NULL,
            title STRING,
            published_at TIMESTAMP,
            category STRING,
            tags ARRAY<STRING>,
            chunk_index INT NOT NULL,
            header_h1 STRING,
            header_h2 STRING,
            header_h3 STRING,
            section_path STRING,
            chunk_text STRING NOT NULL,
            chunk_char_count INT NOT NULL,
            content_hash STRING NOT NULL,
            ingested_at TIMESTAMP NOT NULL
        ) USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """
    )
    spark.sql(
        f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """
    )


def create_silver_blog_posts_table(spark: SparkSession, config: ProjectConfig) -> None:
    """Create or replace the Silver table with cleaned blog post text.

    The Silver table is fully derived from the Bronze table and contains
    the HTML converted to readable text.
    """
    bronze_full = _bronze_table_full_name(config)
    silver_full = _silver_table_full_name(config)

    html_to_text_udf = F.udf(_html_to_text, T.StringType())

    bronze_df: DataFrame = spark.table(bronze_full)
    silver_df = bronze_df.select(
        "url",
        "path_in_storage",
        "title",
        "published_at",
        "category",
        "tags",
        html_to_text_udf(F.col("raw_html")).alias("text"),
        "ingested_at",
    )

    silver_df.write.mode("overwrite").saveAsTable(silver_full)


def create_silver_blog_post_chunks_table(
    spark: SparkSession, config: ProjectConfig
) -> int:
    """Create/update Silver chunk table from Bronze HTML using incremental merge."""
    bronze_full = _bronze_table_full_name(config)
    silver_chunks_full = _silver_chunks_table_full_name(config)
    _ensure_silver_chunks_table(spark, config)

    bronze_df: DataFrame = spark.table(bronze_full).select(
        "url",
        "title",
        "published_at",
        "category",
        "tags",
        "raw_html",
    )
    bronze_rows = list(bronze_df.toLocalIterator())

    chunk_rows: list[dict[str, object]] = []
    for row in bronze_rows:
        chunk_rows.extend(_build_chunk_rows(row))

    schema = T.StructType(
        [
            T.StructField("chunk_id", T.StringType(), False),
            T.StructField("url", T.StringType(), False),
            T.StructField("title", T.StringType(), True),
            T.StructField("published_at", T.TimestampType(), True),
            T.StructField("category", T.StringType(), True),
            T.StructField("tags", T.ArrayType(T.StringType()), True),
            T.StructField("chunk_index", T.IntegerType(), False),
            T.StructField("header_h1", T.StringType(), True),
            T.StructField("header_h2", T.StringType(), True),
            T.StructField("header_h3", T.StringType(), True),
            T.StructField("section_path", T.StringType(), True),
            T.StructField("chunk_text", T.StringType(), False),
            T.StructField("chunk_char_count", T.IntegerType(), False),
            T.StructField("content_hash", T.StringType(), False),
            T.StructField("ingested_at", T.TimestampType(), False),
        ]
    )
    src_df = spark.createDataFrame(chunk_rows, schema=schema)
    src_view = "_silver_blog_post_chunks_src"
    src_df.createOrReplaceTempView(src_view)

    spark.sql(
        f"""
        MERGE INTO {silver_chunks_full} AS t
        USING {src_view} AS s
        ON t.chunk_id = s.chunk_id
        WHEN MATCHED AND t.content_hash <> s.content_hash THEN
          UPDATE SET
            t.url = s.url,
            t.title = s.title,
            t.published_at = s.published_at,
            t.category = s.category,
            t.tags = s.tags,
            t.chunk_index = s.chunk_index,
            t.header_h1 = s.header_h1,
            t.header_h2 = s.header_h2,
            t.header_h3 = s.header_h3,
            t.section_path = s.section_path,
            t.chunk_text = s.chunk_text,
            t.chunk_char_count = s.chunk_char_count,
            t.content_hash = s.content_hash,
            t.ingested_at = s.ingested_at
        WHEN NOT MATCHED THEN
          INSERT *
        """
    )

    spark.sql(
        f"""
        DELETE FROM {silver_chunks_full} AS t
        WHERE t.url IN (SELECT DISTINCT url FROM {src_view})
          AND NOT EXISTS (
            SELECT 1 FROM {src_view} AS s
            WHERE s.chunk_id = t.chunk_id
          )
        """
    )
    return len(chunk_rows)
