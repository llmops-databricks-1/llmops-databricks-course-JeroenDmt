"""Minimal tools for blog agent: search and summarize."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from databricks.vector_search.client import VectorSearchClient
from openai import OpenAI
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from llmops_databricks_course_JeroenDmt.blog_ingestion.write_silver import (
    SILVER_TABLE_NAME,
)
from llmops_databricks_course_JeroenDmt.config.models import ProjectConfig
from llmops_databricks_course_JeroenDmt.vector_search.vector_search import (
    VECTOR_SEARCH_ENDPOINT_NAME,
    _index_name,
)


def _parse_date(date_str: str | None) -> datetime | None:
    if not date_str:
        return None
    return datetime.fromisoformat(date_str)


class _VectorSearchIndex(Protocol):
    def similarity_search(
        self,
        *,
        columns: list[str],
        query_text: str,
        filters: dict[str, Any] | None,
        num_results: int,
    ) -> dict[str, Any]: ...


def _get_index(config: ProjectConfig) -> _VectorSearchIndex:
    client = VectorSearchClient(disable_notice=True)
    index_name = _index_name(config)
    endpoint_name = VECTOR_SEARCH_ENDPOINT_NAME
    return client.get_index(endpoint_name=endpoint_name, index_name=index_name)


def _get_spark() -> SparkSession:
    """Get Spark session from Databricks Connect or from a Databricks job."""
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except Exception:
        pass

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session found.")
    return spark


def _silver_posts_table_name(config: ProjectConfig) -> str:
    return f"{config.catalog}.{config.db_schema}.{SILVER_TABLE_NAME}"


def _fetch_full_posts_by_urls(
    config: ProjectConfig,
    urls: list[str],
) -> list[dict[str, Any]]:
    if not urls:
        return []
    spark = _get_spark()
    table_name = _silver_posts_table_name(config)
    rows = (
        spark.table(table_name)
        .where(F.col("url").isin(urls))
        .select("url", "title", "published_at", "category", "tags", "text")
        .collect()
    )
    return [
        {
            "url": r["url"],
            "title": r["title"],
            "published_at": r["published_at"],
            "category": r["category"],
            "tags": r["tags"],
            "text": r["text"],
        }
        for r in rows
    ]


def _summarize_text_with_llm(text: str, title: str | None = None) -> str:
    prompt = (
        "Summarize this Databricks blog post in 3 concise bullet points. "
        "Focus on key updates, practical implications, and who should care.\n\n"
        f"Title: {title or 'Unknown'}\n"
        f"Content:\n{text}"
    )
    client = OpenAI()
    response = client.responses.create(
        model="gpt-4.1-mini",
        input=prompt,
    )
    return (response.output_text or "").strip()


def search_documents(
    config: ProjectConfig,
    query: str,
    *,
    topic: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    k: int = 8,
) -> list[dict[str, Any]]:
    """Search blog chunks using the existing Vector Search index."""
    index = _get_index(config)

    filters: dict[str, Any] = {}
    if topic:
        filters["category"] = topic
    start_dt = _parse_date(start_date)
    end_dt = _parse_date(end_date)
    if start_dt or end_dt:
        rng: dict[str, Any] = {}
        if start_dt:
            rng["gte"] = start_dt.isoformat()
        if end_dt:
            rng["lte"] = end_dt.isoformat()
        filters["published_at"] = rng

    response = index.similarity_search(
        columns=[
            "url",
            "title",
            "published_at",
            "category",
            "tags",
            "chunk_text",
            "chunk_index",
        ],
        query_text=query,
        filters=filters or None,
        num_results=k,
    )

    rows = response.get("result", {}).get("data_array", [])
    out: list[dict[str, Any]] = []
    for row in rows:
        if len(row) < 7:
            continue
        (
            url,
            title,
            published_at,
            category,
            tags,
            chunk_text,
            chunk_index,
        ) = row[:7]
        out.append(
            {
                "url": url,
                "title": title,
                "published_at": published_at,
                "category": category,
                "tags": tags,
                "chunk_text": chunk_text,
                "chunk_index": chunk_index,
            }
        )
    return out


def summarize_documents(
    config: ProjectConfig,
    urls: list[str],
) -> dict[str, Any]:
    """Summarize full blog posts from `silver_blog_posts` for given URLs."""
    if not urls:
        return {
            "summary": "I could not find any relevant blog posts for this query.",
            "sources": [],
        }

    # Deduplicate URLs and keep insertion order.
    seen: set[str] = set()
    unique_urls: list[str] = []
    for url in urls:
        if url and url not in seen:
            seen.add(url)
            unique_urls.append(url)

    posts = _fetch_full_posts_by_urls(config, unique_urls)
    if not posts:
        return {
            "summary": "I could not find any relevant blog posts for this query.",
            "sources": [],
        }

    bullets: list[str] = []
    for post in posts:
        title = post.get("title") or "Untitled post"
        text = (post.get("text") or "").strip()
        if not text:
            bullets.append(f"- {title}: no text available.")
            continue
        try:
            post_summary = _summarize_text_with_llm(text=text, title=title)
        except Exception:
            fallback = text.split(".")[0].strip()
            post_summary = fallback if fallback else "No summary available."
        bullets.append(f"- {title}: {post_summary}")

    sources = [p["url"] for p in posts if p.get("url")]
    summary = "Overview of relevant blog posts:\n" + "\n\n".join(bullets)
    return {"summary": summary, "sources": sources}
