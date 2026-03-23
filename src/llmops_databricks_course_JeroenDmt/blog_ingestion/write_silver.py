"""Create Silver table for Databricks blog posts from Bronze."""

from bs4 import BeautifulSoup
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from llmops_databricks_course_JeroenDmt.blog_ingestion.write_bronze import (
    _bronze_table_full_name,
)
from llmops_databricks_course_JeroenDmt.config.models import ProjectConfig

SILVER_TABLE_NAME = "silver_blog_posts"


def _silver_table_full_name(config: ProjectConfig) -> str:
    return f"{config.catalog}.{config.db_schema}.{SILVER_TABLE_NAME}"


def _html_to_text(html: str | None) -> str | None:
    """Convert complex blog HTML into readable plain text using lxml parser."""
    if html is None:
        return None
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    # Normalize whitespace
    return " ".join(text.split())


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
