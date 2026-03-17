"""Tests for blog ingestion (fetch + helpers)."""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from llmops_databricks_course_JeroenDmt.blog_ingestion.fetch_blog_posts import (
    _post_id_from_url,
    list_recent_posts,
)


def test_post_id_from_url():
    """post_id is the last path segment of the blog URL."""
    assert _post_id_from_url("https://www.databricks.com/blog/my-post-title") == "my-post-title"
    assert _post_id_from_url("https://www.databricks.com/blog/2025/01/slug") == "slug"
    assert _post_id_from_url("https://www.databricks.com/blog/") == "blog"


@patch("llmops_databricks_course_JeroenDmt.blog_ingestion.fetch_blog_posts.requests.get")
def test_list_recent_posts_parses_rss(mock_get):
    """list_recent_posts parses RSS XML and returns list of post dicts."""
    rss_body = b"""<?xml version="1.0"?>
<rss version="2.0">
<channel>
  <title>Databricks</title>
  <item>
    <title>Test Post</title>
    <link>https://www.databricks.com/blog/test-post</link>
    <pubDate>Thu, 12 Mar 2026 15:00:00 GMT</pubDate>
    <category>Platform</category>
  </item>
</channel>
</rss>"""
    mock_get.return_value.status_code = 200
    mock_get.return_value.content = rss_body
    mock_get.return_value.raise_for_status = lambda: None

    posts = list_recent_posts()
    assert len(posts) == 1
    assert posts[0]["url"] == "https://www.databricks.com/blog/test-post"
    assert posts[0]["post_id"] == "test-post"
    assert posts[0]["title"] == "Test Post"
    assert posts[0]["tags"] == ["Platform"]
    assert posts[0]["category"] == "Platform"
    assert isinstance(posts[0]["published_at"], datetime)
    assert posts[0]["published_at"].tzinfo is not None
