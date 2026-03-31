# Databricks notebook source
"""
Create and test UC SQL functions for blog agent tools.

Functions created:
- kb_search_documents_sql(query, start_date, end_date)
- kb_summarize_documents_sql(urls)
"""

# COMMAND ----------

# COMMAND ----------

dbutils.widgets.text("catalog", "llmops_dev")
dbutils.widgets.text("schema", "databricks_kb")
dbutils.widgets.text(
    "index_name",
    "llmops_dev.databricks_kb.silver_blog_post_chunks_index",
)

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
index_name = dbutils.widgets.get("index_name").strip()

if not catalog or not schema or not index_name:
    raise ValueError(
        "Please provide non-empty catalog, schema, and index_name."
    )

print(f"catalog={catalog}")
print(f"schema={schema}")
print(f"index_name={index_name}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# Create search function (TVF) backed by Vector Search index.
spark.sql(
    f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.kb_search_documents_sql(
  query STRING,
  start_date DATE,
  end_date DATE
)
RETURNS TABLE (
  url STRING,
  title STRING,
  published_at TIMESTAMP,
  category STRING,
  tags ARRAY<STRING>,
  chunk_text STRING,
  chunk_index INT
)
RETURN
SELECT
  CAST(v.url AS STRING) AS url,
  CAST(v.title AS STRING) AS title,
  CAST(v.published_at AS TIMESTAMP) AS published_at,
  CAST(v.category AS STRING) AS category,
  CAST(v.tags AS ARRAY<STRING>) AS tags,
  CAST(v.chunk_text AS STRING) AS chunk_text,
  CAST(v.chunk_index AS INT) AS chunk_index
FROM vector_search(
  index => '{index_name}',
  query => query,
  num_results => 25
) AS v
WHERE 
  (start_date IS NULL OR CAST(v.published_at AS DATE) >= start_date)
  AND (end_date IS NULL OR CAST(v.published_at AS DATE) <= end_date)
"""
)

# COMMAND ----------

# Create summarize function (scalar) from full documents in silver_blog_posts.
# Returns one combined string with a separate summary per URL.
spark.sql(
    f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.kb_summarize_documents_sql(
  urls ARRAY<STRING>
)
RETURNS STRING
RETURN (
  WITH docs AS (
    SELECT
      url,
      title,
      text,
      published_at
    FROM {catalog}.{schema}.silver_blog_posts
    WHERE array_contains(urls, url)
  ),
  per_doc AS (
    SELECT
      url,
      title,
      published_at,
      ai_summarize(coalesce(text, '')) AS summary
    FROM docs
  )
  SELECT concat_ws(
    '\\n\\n',
    transform(
      array_sort(
        collect_list(
          named_struct(
            'published_at', published_at,
            'url', url,
            'title', coalesce(title, 'Untitled'),
            'summary', coalesce(summary, 'No summary available.')
          )
        )
      ),
      x -> concat(
        'Title: ', x.title, '\\n',
        'URL: ', x.url, '\\n',
        x.summary
      )
    )
  )
  FROM per_doc
)
"""
)

# Test 1: run SQL search function directly.
spark.sql(
    f"""
SELECT *
FROM {catalog}.{schema}.kb_search_documents_sql(
  'unity catalog governance best practices',
  DATE('2025-01-01'),
  CURRENT_DATE()
)
"""
).display()

# COMMAND ----------

# Test 2: run SQL summarize function directly with explicit URLs.
spark.sql(
    f"""
SELECT {catalog}.{schema}.kb_summarize_documents_sql(
  array(
    'https://www.databricks.com/blog',
    'https://www.databricks.com/blog/'
  )
) AS summary_text
"""
).display()

# COMMAND ----------

# Test 3: compose SQL functions in SQL (search -> summarize).
spark.sql(
    f"""
WITH hits AS (
  SELECT url
  FROM {catalog}.{schema}.kb_search_documents_sql(
    'delta live tables recent updates',
    DATE('2025-01-01'),
    CURRENT_DATE()
  )
),
urls AS (
  SELECT collect_set(url) AS url_array
  FROM hits
)
SELECT {catalog}.{schema}.kb_summarize_documents_sql(url_array) AS summary_text
FROM urls
"""
).display()

# COMMAND ----------
