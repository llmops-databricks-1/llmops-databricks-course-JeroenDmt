"""Tests for project config loader."""

import pytest

from llmops_databricks_course_JeroenDmt.config import (
    EnvName,
    get_config_path,
    load_project_config,
)


@pytest.fixture
def config_path():
    """Path to project_config.yml; skip if missing."""
    path = get_config_path()
    if not path.exists():
        pytest.skip("project_config.yml not found at repo root")
    return path


def test_get_config_path_returns_path(config_path):
    """get_config_path() returns a path that exists when file is present."""
    assert get_config_path().exists()
    assert get_config_path().name == "project_config.yml"


def test_load_project_config_dev(config_path):
    """load_project_config(env='dev') returns dev catalog, schema, volumes."""
    config = load_project_config(path=config_path, env="dev")
    assert config.catalog == "llmops_dev"
    assert config.db_schema == "databricks_kb"
    assert config.raw_root == "/Volumes/llmops_dev/databricks_kb/databricks_kb_raw"
    assert config.blog_html_root == (
        "/Volumes/llmops_dev/databricks_kb/databricks_kb_raw/blog_posts_html"
    )
    assert config.docs_pdf_root == "/Volumes/llmops_dev/databricks_kb/databricks_kb_raw/docs_pdf"


def test_load_project_config_uat(config_path):
    """load_project_config(env='uat') returns uat settings."""
    config = load_project_config(path=config_path, env="uat")
    assert config.catalog == "llmops_uat"
    assert config.db_schema == "databricks_kb"


def test_load_project_config_prd(config_path):
    """load_project_config(env='prd') returns prd settings."""
    config = load_project_config(path=config_path, env="prd")
    assert config.catalog == "llmops_prd"
    assert config.db_schema == "databricks_kb"


def test_load_project_config_default_env(config_path):
    """load_project_config() without env defaults to dev."""
    config = load_project_config(path=config_path)
    assert config.catalog == "llmops_dev"


def test_load_project_config_invalid_env_raises(config_path):
    """load_project_config(env='invalid') raises KeyError."""
    with pytest.raises(KeyError, match="Environment .* not found"):
        load_project_config(path=config_path, env="invalid")  # type: ignore[arg-type]


def test_bronze_table_name_from_config(config_path):
    """Example: build Bronze table name from config for blog posts."""
    config = load_project_config(path=config_path, env="dev")
    bronze_table = f"{config.catalog}.{config.db_schema}.bronze_blog_posts"
    assert bronze_table == "llmops_dev.databricks_kb.bronze_blog_posts"
