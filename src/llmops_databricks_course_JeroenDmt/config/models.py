"""Pydantic model for project_config.yml (single environment)."""

from typing import Literal

from pydantic import BaseModel, computed_field, Field

EnvName = Literal["dev", "uat", "prd"]


class ProjectConfig(BaseModel):
    """Environment-specific project configuration (catalog, schema, volumes, ingestion)."""

    catalog: str
    db_schema: str = Field(alias="schema")
    raw_root: str
    blog_html_subdir: str
    docs_pdf_subdir: str
    platform_release_notes_subdir: str
    blog_max_age_months: int = 12

    @computed_field
    @property
    def blog_html_root(self) -> str:
        return f"{self.raw_root}/{self.blog_html_subdir}"

    @computed_field
    @property
    def docs_pdf_root(self) -> str:
        return f"{self.raw_root}/{self.docs_pdf_subdir}"

    @computed_field
    @property
    def platform_release_notes_root(self) -> str:
        return f"{self.raw_root}/{self.platform_release_notes_subdir}"
