"""Load project_config.yml and return config for a single environment."""

from pathlib import Path

import yaml

from llmops_databricks_course_JeroenDmt.config.models import EnvName, ProjectConfig


def get_config_path() -> Path:
    """
    Resolve path to project_config.yml.

    Used only as a fallback for local development; Databricks jobs
    should pass an explicit path via load_project_config(path=...).
    """
    this_file = Path(__file__).resolve()
    repo_root = this_file.parents[3]
    return repo_root / "project_config.yml"


def load_project_config(
    path: Path | None = None,
    env: EnvName = "dev",
) -> ProjectConfig:
    """Load project_config.yml and return config for the given environment."""
    config_path = path if path is not None else get_config_path()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not data:
        raise ValueError(f"Config file is empty: {config_path}")

    if env not in data:
        raise KeyError(f"Environment {env!r} not found in config (keys: {list(data)})")

    env_data = data[env]
    # Flatten volumes into one dict for ProjectConfig
    flat = {
        "catalog": env_data["catalog"],
        "schema": env_data["schema"],
        **env_data["volumes"],
    }
    return ProjectConfig(**flat)
