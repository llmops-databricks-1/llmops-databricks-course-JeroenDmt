# Databricks notebook source
"""
Interactive notebook to test llmops_databricks_course_JeroenDmt.blog_agent_answer().
"""

# COMMAND ----------

import shlex
import sys
from pathlib import Path

import llmops_databricks_course_JeroenDmt as app

# For direct tool testing
from llmops_databricks_course_JeroenDmt.agent.tools import (
    search_documents,
    summarize_documents,
)
from llmops_databricks_course_JeroenDmt.config import load_project_config

# COMMAND ----------

# Databricks widgets for quick interactive testing.
dbutils.widgets.text("env", "dev")
dbutils.widgets.text(
    "config_path",
    "/Workspace/Users/<your-user>/.bundle/llmops-databricks-course-JeroenDmt/dev/files/project_config.yml",
)
dbutils.widgets.text(
    "query",
    "Provide an overview of recent blog posts on Lakebase",
)

# COMMAND ----------

env = dbutils.widgets.get("env").strip()
config_path = dbutils.widgets.get("config_path").strip()
query = dbutils.widgets.get("query").strip()

if not query:
    raise ValueError("Please provide a non-empty query in the 'query' widget.")

argv = [
    "blog_agent_answer",
    "--env",
    env,
]

if config_path:
    argv.extend(["--config-path", config_path])

argv.extend(shlex.split(query))

print("Running blog_agent_answer with:")
print(f"  env={env}")
print(f"  config_path={config_path or '<default>'}")
print(f"  query={query}")

# COMMAND ----------

old_argv = sys.argv
try:
    sys.argv = argv
    exit_code = app.blog_agent_answer()
finally:
    sys.argv = old_argv

print(f"\nblog_agent_answer exit_code={exit_code}")

# COMMAND ----------

# Optional: test the raw search_documents tool.

if config_path:
    _config = load_project_config(path=Path(config_path), env=env)
else:
    _config = load_project_config(env=env)

print("\nTesting search_documents...")
search_results = search_documents(_config, query=query, k=5)
print(f"Retrieved {len(search_results)} chunks.")
for row in search_results[:3]:
    print(f"- {row['title']} ({row['url']})")

# COMMAND ----------

# Optional: test summarize_documents directly on URLs from search.

urls = [r["url"] for r in search_results]
print("\nTesting summarize_documents on full posts...")
summary_result = summarize_documents(_config, urls)
print(summary_result["summary"])
print("\nSources used:")
for url in summary_result["sources"]:
    print(f"- {url}")
