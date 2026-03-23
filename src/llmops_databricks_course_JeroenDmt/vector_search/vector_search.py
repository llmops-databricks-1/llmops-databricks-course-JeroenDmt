"""Create and sync the Vector Search index for blog post chunks."""

from databricks.vector_search.client import VectorSearchClient

from llmops_databricks_course_JeroenDmt.config.models import ProjectConfig

VECTOR_SEARCH_ENDPOINT_NAME = "vector-search-endpoint"
EMBEDDING_MODEL_ENDPOINT_NAME = "databricks-gte-large-en"
CHUNKS_INDEX_BASENAME = "silver_blog_post_chunks_index"
CHUNKS_TABLE_BASENAME = "silver_blog_post_chunks"
PRIMARY_KEY_COLUMN = "chunk_id"
EMBEDDING_SOURCE_COLUMN = "chunk_text"


def _source_table_name(config: ProjectConfig) -> str:
    return f"{config.catalog}.{config.db_schema}.{CHUNKS_TABLE_BASENAME}"


def _index_name(config: ProjectConfig) -> str:
    return f"{config.catalog}.{config.db_schema}.{CHUNKS_INDEX_BASENAME}"


def _list_with_name(items: list[dict[str, object]]) -> set[str]:
    names: set[str] = set()
    for item in items:
        name = item.get("name")
        if isinstance(name, str):
            names.add(name)
    return names


def _endpoint_exists(client: VectorSearchClient, endpoint_name: str) -> bool:
    response = client.list_endpoints()
    endpoints = response.get("endpoints", [])
    if not isinstance(endpoints, list):
        return False
    endpoint_names = _list_with_name([x for x in endpoints if isinstance(x, dict)])
    return endpoint_name in endpoint_names


def _first_endpoint_name(client: VectorSearchClient) -> str | None:
    response = client.list_endpoints()
    endpoints = response.get("endpoints", [])
    if not isinstance(endpoints, list):
        return None
    endpoint_names = sorted(
        _list_with_name([x for x in endpoints if isinstance(x, dict)])
    )
    if not endpoint_names:
        return None
    return endpoint_names[0]


def _resolve_endpoint_name(client: VectorSearchClient) -> str:
    endpoint_name = VECTOR_SEARCH_ENDPOINT_NAME
    if _endpoint_exists(client, endpoint_name):
        return endpoint_name

    try:
        client.create_endpoint_and_wait(name=endpoint_name, endpoint_type="STANDARD")
        return endpoint_name
    except Exception as e:  # pragma: no cover - depends on workspace quota/state
        message = str(e)
        if "QUOTA_EXCEEDED" not in message:
            raise
        fallback = _first_endpoint_name(client)
        if fallback is None:
            raise
        return fallback


def _index_exists(
    client: VectorSearchClient,
    endpoint_name: str,
    index_name: str,
) -> bool:
    response = client.list_indexes(endpoint_name)
    indexes = response.get("vector_indexes", [])
    if not isinstance(indexes, list):
        return False
    index_names = _list_with_name([x for x in indexes if isinstance(x, dict)])
    return index_name in index_names


def ensure_and_sync_chunks_index(config: ProjectConfig) -> tuple[str, str, bool]:
    """
    Ensure a triggered Delta Sync index exists and run sync.

    Returns tuple(endpoint_name, index_name, created_now).
    """
    client = VectorSearchClient(disable_notice=True)
    endpoint_name = _resolve_endpoint_name(client)
    index_name = _index_name(config)
    source_table = _source_table_name(config)

    created_now = False
    if not _index_exists(client, endpoint_name, index_name):
        client.create_delta_sync_index_and_wait(
            endpoint_name=endpoint_name,
            index_name=index_name,
            primary_key=PRIMARY_KEY_COLUMN,
            source_table_name=source_table,
            pipeline_type="TRIGGERED",
            embedding_source_column=EMBEDDING_SOURCE_COLUMN,
            embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT_NAME,
            columns_to_sync=[
                "url",
                "title",
                "published_at",
                "category",
                "tags",
                "chunk_index",
                "header_h1",
                "header_h2",
                "header_h3",
                "section_path",
                "chunk_text",
                "chunk_char_count",
                "content_hash",
                "ingested_at",
            ],
        )
        created_now = True
    else:
        index = client.get_index(endpoint_name=endpoint_name, index_name=index_name)
        index.sync()

    return endpoint_name, index_name, created_now
