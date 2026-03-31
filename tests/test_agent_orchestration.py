"""Tests for the minimal blog agent orchestrator."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from llmops_databricks_course_JeroenDmt.agent.orchestrator import answer_question
from llmops_databricks_course_JeroenDmt.agent.tools import summarize_documents
from llmops_databricks_course_JeroenDmt.config.models import ProjectConfig


def _dummy_config() -> ProjectConfig:
    return ProjectConfig(
        catalog="llmops_dev",
        schema="databricks_kb",
        raw_root="/Volumes/llmops_dev/databricks_kb/databricks_kb_raw",
        blog_html_subdir="blog",
        docs_pdf_subdir="docs",
        platform_release_notes_subdir="rn",
    )


def test_summarize_documents_deduplicates_url_input() -> None:
    config = _dummy_config()
    urls = ["https://example.com/a", "https://example.com/a", "https://example.com/b"]

    mocked_posts: list[dict[str, Any]] = [
        {
            "url": "https://example.com/a",
            "title": "Post A",
            "text": "This is content A.",
        },
        {
            "url": "https://example.com/b",
            "title": "Post B",
            "text": "This is content B.",
        },
    ]

    with (
        patch(
            "llmops_databricks_course_JeroenDmt.agent.tools._fetch_full_posts_by_urls",
            return_value=mocked_posts,
        ),
        patch(
            "llmops_databricks_course_JeroenDmt.agent.tools._summarize_text_with_llm",
            side_effect=["Summary A", "Summary B"],
        ),
    ):
        result = summarize_documents(config, urls)

    assert "Summary A" in result["summary"]
    assert "Summary B" in result["summary"]
    assert result["sources"] == ["https://example.com/a", "https://example.com/b"]


def test_answer_question_empty_results_graceful() -> None:
    config = _dummy_config()

    with (
        patch(
            "llmops_databricks_course_JeroenDmt.agent.orchestrator.search_documents",
            return_value=[],
        ),
        patch(
            "llmops_databricks_course_JeroenDmt.agent.orchestrator.summarize_documents",
            return_value={
                "summary": "I could not find any relevant blog posts for this query.",
                "sources": [],
            },
        ),
    ):
        result = answer_question(config, "nonexistent topic")

    assert "could not find any relevant blog posts" in result["answer"]
    assert result["sources"] == []
    assert result["num_docs"] == 0

