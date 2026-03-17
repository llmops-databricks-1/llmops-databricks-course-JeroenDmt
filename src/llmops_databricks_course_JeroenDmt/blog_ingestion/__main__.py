"""
CLI entry point for bronze blog ingestion.

Usage:
  python -m llmops_databricks_course_JeroenDmt.blog_ingestion --env dev
"""

from llmops_databricks_course_JeroenDmt.__init__ import (  # type: ignore[import-not-found]
    blog_ingestion as blog_ingestion_entrypoint,
)


def main() -> int:
    # Delegate to the top-level blog_ingestion() function exported by the package.
    return blog_ingestion_entrypoint()


if __name__ == "__main__":
    raise SystemExit(main())
