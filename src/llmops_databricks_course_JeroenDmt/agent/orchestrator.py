"""Minimal orchestrator for answering blog questions."""

from __future__ import annotations

from typing import Any

from llmops_databricks_course_JeroenDmt.agent.tools import (
    search_documents,
    summarize_documents,
)
from llmops_databricks_course_JeroenDmt.config.models import ProjectConfig


def answer_question(config: ProjectConfig, query: str) -> dict[str, Any]:
    """Answer a user question using vector search + summarization."""
    docs = search_documents(config, query=query, k=8)
    # Summarization tool takes URLs only and uses full Silver documents.
    urls = [str(d["url"]) for d in docs if d.get("url")]
    result = summarize_documents(config, urls)
    return {
        "answer": result["summary"],
        "sources": result["sources"],
        "num_docs": len(docs),
    }
