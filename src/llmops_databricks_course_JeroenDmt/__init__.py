"""llmops-databricks-course-JeroenDmt - LLMOps Course on Databricks."""

__version__ = "0.0.1"


from pathlib import Path

from pyspark.sql import SparkSession


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


def _parse_common_args() -> tuple[str, Path | None]:
    import sys

    env = "dev"
    config_path: Path | None = None
    argv = getattr(sys, "argv", [])
    i = 0
    while i < len(argv):
        if argv[i] == "--env" and i + 1 < len(argv):
            env = argv[i + 1]
            i += 2
            continue
        if argv[i] == "--config-path" and i + 1 < len(argv):
            config_path = Path(argv[i + 1])
            i += 2
            continue
        i += 1
    return env, config_path


def blog_ingestion() -> int:
    """
    Entry point for Databricks job: run Bronze blog ingestion.

    Called by the job as module.blog_ingestion(). Job parameters are passed as CLI-style
    args.
    """
    from llmops_databricks_course_JeroenDmt.blog_ingestion.write_bronze import (
        run_blog_ingestion,
    )
    from llmops_databricks_course_JeroenDmt.config import load_project_config

    env, config_path = _parse_common_args()
    if config_path is not None:
        config = load_project_config(path=config_path, env=env)
    else:
        config = load_project_config(env=env)
    spark = _get_spark()
    n = run_blog_ingestion(spark, config)
    print(f"Ingested {n} blog posts into Bronze for env '{env}'.")
    return n


def blog_ingestion_silver() -> int:
    """Silver layer entry point: derive text table from Bronze."""
    from llmops_databricks_course_JeroenDmt.blog_ingestion.write_silver import (
        create_silver_blog_posts_table,
    )
    from llmops_databricks_course_JeroenDmt.config import load_project_config

    env, config_path = _parse_common_args()
    if config_path is not None:
        config = load_project_config(path=config_path, env=env)
    else:
        config = load_project_config(env=env)
    spark = _get_spark()
    create_silver_blog_posts_table(spark, config)
    print(f"Refreshed Silver blog posts table for env '{env}'.")
    return 0


def blog_ingestion_silver_chunks() -> int:
    """Silver layer entry point: derive chunk table from Bronze."""
    from llmops_databricks_course_JeroenDmt.blog_ingestion.write_silver import (
        create_silver_blog_post_chunks_table,
    )
    from llmops_databricks_course_JeroenDmt.config import load_project_config

    env, config_path = _parse_common_args()
    if config_path is not None:
        config = load_project_config(path=config_path, env=env)
    else:
        config = load_project_config(env=env)
    spark = _get_spark()
    n_chunks = create_silver_blog_post_chunks_table(spark, config)
    print(f"Refreshed Silver blog post chunks ({n_chunks}) for env '{env}'.")
    return 0


def blog_ingestion_vector_index() -> int:
    """Vector Search entry point: create/sync index from Silver chunks."""
    from llmops_databricks_course_JeroenDmt.config import load_project_config
    from llmops_databricks_course_JeroenDmt.vector_search.vector_search import (
        EMBEDDING_MODEL_ENDPOINT_NAME,
        ensure_and_sync_chunks_index,
    )

    env, config_path = _parse_common_args()
    if config_path is not None:
        config = load_project_config(path=config_path, env=env)
    else:
        config = load_project_config(env=env)

    endpoint_name, index_name, created_now = ensure_and_sync_chunks_index(config)
    action = "Created and synced" if created_now else "Synced"
    print(
        f"{action} Vector Search index '{index_name}' on endpoint "
        f"'{endpoint_name}' using embeddings "
        f"'{EMBEDDING_MODEL_ENDPOINT_NAME}' for env '{env}'."
    )
    return 0
