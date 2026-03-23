"""Write blog HTML to UC volume and upsert Bronze rows."""

import io
from datetime import UTC, datetime
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType
from pyspark.sql import SparkSession

from llmops_databricks_course_JeroenDmt.config.models import ProjectConfig

BRONZE_TABLE_NAME = "bronze_blog_posts"


def _parse_volume_root(raw_root: str) -> tuple[str, str, str]:
    """
    Extract (catalog, schema, volume_name) from a raw_root like
    /Volumes/<catalog>/<schema>/<volume_name>.
    """
    # Strip leading slash and split
    path = raw_root.lstrip("/")
    parts = path.split("/")
    if len(parts) < 4 or parts[0] != "Volumes":
        raise ValueError(
            "raw_root must start with /Volumes/<catalog>/<schema>/<volume>, "
            f"got {raw_root!r}"
        )
    _, catalog, schema, volume_name = parts[:4]
    return catalog, schema, volume_name


def ensure_catalog_schema_volume(
    config: ProjectConfig, client: WorkspaceClient | None = None
) -> None:
    """
    Ensure that the catalog, schema, and volume for raw_root exist.

    This is safe to call repeatedly; all operations are idempotent.
    """
    w = client or WorkspaceClient()
    catalog, schema, volume_name = _parse_volume_root(config.raw_root)

    # Catalog
    try:
        w.catalogs.get(name=catalog)
    except NotFound:
        w.catalogs.create(name=catalog)

    # Schema
    schema_full = f"{catalog}.{schema}"
    try:
        w.schemas.get(full_name=schema_full)
    except NotFound:
        w.schemas.create(name=schema, catalog_name=catalog)

    # Volume: VolumesAPI has create/list but no get, so list and check by name.
    existing = [
        v
        for v in w.volumes.list(catalog_name=catalog, schema_name=schema)
        if v.name == volume_name
    ]
    if not existing:
        w.volumes.create(
            name=volume_name,
            catalog_name=catalog,
            schema_name=schema,
            volume_type=VolumeType.MANAGED,
        )


def write_html_to_volume(
    config: ProjectConfig,
    post: dict[str, Any],
    html: str,
    *,
    client: WorkspaceClient | None = None,
) -> str:
    """
    Write blog HTML to the UC volume at blog_html_root/YYYY/MM/post_id.html.

    Returns the full volume path (path_in_storage).
    """
    w = client or WorkspaceClient()
    # Ensure UC objects exist before we try to write to the volume
    ensure_catalog_schema_volume(config, client=w)
    published_at = post.get("published_at")
    if isinstance(published_at, datetime):
        year, month = published_at.year, published_at.month
    else:
        now = datetime.now(UTC)
        year, month = now.year, now.month
    post_id = post["post_id"]
    dir_path = f"{config.blog_html_root}/{year}/{month:02d}"
    file_path = f"{dir_path}/{post_id}.html"

    w.files.create_directory(directory_path=dir_path)
    w.files.upload(
        file_path=file_path,
        contents=io.BytesIO(html.encode("utf-8")),
        overwrite=True,
    )
    return file_path


def _bronze_table_full_name(config: ProjectConfig) -> str:
    return f"{config.catalog}.{config.db_schema}.{BRONZE_TABLE_NAME}"


def ensure_bronze_table(spark: SparkSession, config: ProjectConfig) -> None:
    """Create the Bronze table if it does not exist (Unity Catalog).

    Assumes that the catalog and schema already exist; callers should invoke
    ensure_catalog_schema_volume() first when running outside of Databricks.
    """
    full_name = _bronze_table_full_name(config)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
            url STRING NOT NULL,
            path_in_storage STRING,
            title STRING,
            published_at TIMESTAMP,
            category STRING,
            tags ARRAY<STRING>,
            raw_html STRING,
            ingested_at TIMESTAMP NOT NULL
        )
        USING DELTA
        """
    )


def upsert_bronze_row(
    spark: SparkSession,
    config: ProjectConfig,
    post: dict[str, Any],
    path_in_storage: str,
    html: str,
) -> None:
    """
    Insert or update one Bronze row for a blog post.

    Uses MERGE on source_type = 'blog' and source_id = post URL.
    """
    from pyspark.sql import Row
    from pyspark.sql.types import (
        ArrayType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    full_name = _bronze_table_full_name(config)
    url = post["url"]
    title = post.get("title") or ""
    category = post.get("category")
    tags = post.get("tags") or []
    published_at = post.get("published_at")
    pub_ts = published_at if isinstance(published_at, datetime) else None
    ingested_at = datetime.now(UTC)
    # Cap very large HTML for storage
    html_stored = html[:1_000_000] if len(html) > 1_000_000 else html

    schema = StructType(
        [
            StructField("url", StringType(), False),
            StructField("path_in_storage", StringType(), True),
            StructField("title", StringType(), True),
            StructField("published_at", TimestampType(), True),
            StructField("category", StringType(), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("raw_html", StringType(), True),
            StructField("ingested_at", TimestampType(), False),
        ]
    )
    row = Row(
        url=url,
        path_in_storage=path_in_storage,
        title=title,
        published_at=pub_ts,
        category=category,
        tags=tags,
        raw_html=html_stored,
        ingested_at=ingested_at,
    )
    src_df = spark.createDataFrame([row], schema=schema)
    src_df.createOrReplaceTempView("_bronze_blog_src")

    spark.sql(
        f"""
        MERGE INTO {full_name} AS t
        USING _bronze_blog_src AS s
        ON t.url = s.url
        WHEN MATCHED AND t.published_at <> s.published_at THEN UPDATE SET
            t.path_in_storage = s.path_in_storage,
            t.title = s.title,
            t.published_at = s.published_at,
            t.category = s.category,
            t.tags = s.tags,
            t.raw_html = s.raw_html,
            t.ingested_at = s.ingested_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def run_blog_ingestion(
    spark: SparkSession,
    config: ProjectConfig,
    *,
    client: WorkspaceClient | None = None,
) -> int:
    """
    List recent blog posts, download HTML, write to volume, and upsert Bronze rows.

    Returns the number of posts processed (written + upserted).
    """
    from llmops_databricks_course_JeroenDmt.blog_ingestion.fetch_blog_posts import (
        download_html,
        list_recent_posts,
    )

    w = client or WorkspaceClient()
    # First run: ensure UC catalog/schema/volume exist before creating tables
    ensure_catalog_schema_volume(config, client=w)
    ensure_bronze_table(spark, config)
    posts = list_recent_posts()
    count = 0
    for post in posts:
        html = download_html(post["url"])
        path_in_storage = write_html_to_volume(config, post, html, client=w)
        upsert_bronze_row(spark, config, post, path_in_storage, html)
        count += 1
    return count
