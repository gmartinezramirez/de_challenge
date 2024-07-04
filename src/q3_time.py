import logging
from datetime import date
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
QUERY_PRIORITY: bigquery.QueryPriority = bigquery.QueryPriority.INTERACTIVE
USE_LEGACY_SQL: bool = False
GCP_CLIENT = bigquery.Client(project=PROJECT_ID)
RETRY_CONFIG = retry.Retry(deadline=30)

JOB_CONFIG = bigquery.QueryJobConfig(
    use_query_cache=USE_QUERY_CACHE,
    priority=QUERY_PRIORITY,
    use_legacy_sql=USE_LEGACY_SQL,
)

Q3_TIME_QUERY = """
WITH MentionsExtracted AS (
  SELECT
    SUBSTR(word, 2) AS username
  FROM (
    SELECT
      content
    FROM
      `{project}.{dataset}.{table}`
    WHERE
      REGEXP_CONTAINS(content, r'@')
  ),
  UNNEST(SPLIT(LOWER(content), ' ')) AS word
  WHERE
    STARTS_WITH(word, '@')
    AND LENGTH(word) > 1
)
SELECT
  username,
  COUNT(*) AS mention_count
FROM
  MentionsExtracted
GROUP BY
  username
ORDER BY
  mention_count DESC
LIMIT 10
"""


def q3_time(file_path: str) -> List[Tuple[date, str]]:
    """
    Q3 Time:
        El top 10 histórico de usuarios (username) más influyentes
        en función del conteo de las menciones (@) que registra cada uno de ellos
        Ejemplo de output:
            [("LATAM321", 387), ("LATAM_CHI", 129), ...]
        q3_memory ejecuta la consulta Q3_TIME_QUERY que resuelve lo anterior
        con un enfoque eficiente en tiempo de ejecución.

    Args:
        file_path (str): No se usa en esta implementación
        se mantiene por consistencia con la firma de la función.
    Returns:
        List[Tuple[date, str]]: Una lista de tuplas que contienen la fecha
                                y el usuario más activo para cada fecha.
    Raises:
        GoogleCloudError: Si hay un error con la API de Google Cloud.
    """
    logger.info("Starting: q3_time")
    query = Q3_TIME_QUERY.format(
        project=PROJECT_ID, dataset=DATASET_ID, table=TABLE_NAME
    )

    try:
        query_job, client_execution_time = execute_query_with_benchmark(
            GCP_CLIENT, query, JOB_CONFIG
        )
        print_job_details(query_job, client_execution_time)
        results = [(row.username, row.mention_count) for row in query_job.result()]
        logger.info("Sucessful finish: q3_time")
        return results
    except GoogleCloudError as e:
        print(f"Error en Bigquery: {str(e)}")
        raise


if __name__ == "__main__":
    result = q3_time("something")
    print(result)
