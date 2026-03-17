"""Fetch Databricks blog post list from RSS and download article HTML."""

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests

BLOG_RSS_URL = "https://www.databricks.com/feed"


def _parse_rss_date(date_str: str) -> datetime | None:
    """Parse RSS pubDate (RFC 2822) to timezone-aware datetime. Assumes UTC."""
    s = date_str.strip()
    # Parse without timezone then attach UTC (RSS times are typically GMT/UTC)
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S"):
        try:
            if fmt.endswith(" %Z"):
                # Strip timezone token for parsing
                s_naive = s.rsplit(" ", 1)[0] if " " in s else s
                dt = datetime.strptime(s_naive, "%a, %d %b %Y %H:%M:%S")
            else:
                dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _post_id_from_url(url: str) -> str:
    """Derive stable post_id from blog URL (last path segment)."""
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1] if path else url


def list_recent_posts() -> list[dict[str, Any]]:
    """
    Fetch latest blog posts from the Databricks RSS feed.

    Returns a list of dicts with keys: url, post_id, title, published_at, tags, category.
    Note: the RSS feed itself limits how many posts are returned.
    """
    resp = requests.get(BLOG_RSS_URL, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    # RSS 2.0: channel/item; strip namespace for simplicity
    def local_tag(elem: ET.Element) -> str:
        return elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

    channel = None
    for child in root:
        if local_tag(child) == "channel":
            channel = child
            break
    if channel is None:
        return []

    posts: list[dict[str, Any]] = []
    for item in channel:
        if local_tag(item) != "item":
            continue
        link_el = next((c for c in item if local_tag(c) == "link"), None)
        link = link_el.text.strip() if link_el is not None and link_el.text else None
        if not link or "/blog/" not in link:
            continue

        title_el = next((c for c in item if local_tag(c) == "title"), None)
        title = (title_el.text or "").strip() if title_el is not None else ""

        pub_el = next((c for c in item if local_tag(c) == "pubDate"), None)
        pub_date = None
        if pub_el is not None and pub_el.text:
            pub_date = _parse_rss_date(pub_el.text)
        if pub_date is None:
            pub_date = datetime.now(timezone.utc)

        categories = [
            c.text.strip()
            for c in item
            if local_tag(c) == "category" and c.text
        ]

        posts.append({
            "url": link,
            "post_id": _post_id_from_url(link),
            "title": title,
            "published_at": pub_date,
            "tags": categories,
            "category": categories[0] if categories else None,
        })

    return posts


def download_html(url: str) -> str:
    """Download the blog article HTML content as a string."""
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text
