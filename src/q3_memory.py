import logging
from typing import List, Tuple

from google.api_core import retry
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from gcp_benchmark import (  # pylint: disable=import-error
    execute_query_with_benchmark,
    print_job_details,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Project config
PROJECT_ID: str = "de-challenge-gm"
DATASET_ID: str = "tweets"
TABLE_NAME: str = "farmers-protest-tweets"
# Job config
USE_QUERY_CACHE: bool = False
# bigquery.QueryPriority.BATCH o bigquery.QueryPriority.INTERACTIVE
QUERY_PRIORITY: bigquery.QueryPriority = bigquery.QueryPriority.BATCH
USE_LEGACY_SQL: bool = False
GCP_CLIENT = bigquery.Client(project=PROJECT_ID)
RETRY_CONFIG = retry.Retry(deadline=30)

JOB_CONFIG = bigquery.QueryJobConfig(
    use_query_cache=USE_QUERY_CACHE,
    priority=QUERY_PRIORITY,
    use_legacy_sql=USE_LEGACY_SQL,
)

Q3_MEMORY_QUERY: str = """
-- Main Query: extraer, contar y ordenar las menciones de usuarios
SELECT
  username,
  COUNT(*) AS mention_count
FROM (
  SELECT
    mentionedUser.username AS username
  FROM
    `{file_path}`,
    UNNEST(mentionedUsers) AS mentionedUser
  WHERE mentionedUsers IS NOT NULL
) AS usernames_table
GROUP BY
  username
ORDER BY
  mention_count DESC
LIMIT
  10;
"""


def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Q3 Memory:
        El top 10 histórico de usuarios (username) más influyentes
        en función del conteo de las menciones (@) que registra cada uno de ellos
        Ejemplo de output:
            [("LATAM321", 387), ("LATAM_CHI", 129), ...]
        q3_memory ejecuta la consulta Q3_MEMORY_QUERY que resuelve lo anterior
        con un enfoque eficiente en uso de memoria.

    Args:
        file_path (str): project.dataset.table en bigquery
    Returns:
        List[Tuple[str, int]]: Una lista de tuplas que contienen el nombre de usuario
        y el conteo de menciones.
    Raises:
        GoogleCloudError: Si hay un error con la API de Google Cloud.
    """
    logger.info("Starting: q3_memory")
    query = Q3_MEMORY_QUERY.format(file_path=file_path)

    try:
        query_job, client_execution_time = execute_query_with_benchmark(
            GCP_CLIENT, query, JOB_CONFIG
        )
        print_job_details(query_job, client_execution_time)
        results = [(row.username, row.mention_count) for row in query_job.result()]
        logger.info("Sucessful finish: q3_memory")
        return results
    except GoogleCloudError as e:
        print(f"Error en Bigquery: {str(e)}")
        raise


if __name__ == "__main__":
    bq_file_path: str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    result: List[Tuple[str, int]] = q3_memory(bq_file_path)
    print(result)
