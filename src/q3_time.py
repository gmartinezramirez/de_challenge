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
-- Función: extraer menciones
CREATE TEMP FUNCTION ExtractMentions(content STRING)
RETURNS ARRAY<STRING>
LANGUAGE js AS '''
  return (content.match(/@[a-zA-Z0-9_]+/g) || []).map(m => m.slice(1).toLowerCase());
''';

-- CTE para extraer y contar menciones en un step
WITH MentionCounts AS (
  SELECT
    mention AS username,
    COUNT(*) AS mention_count
  FROM
    `{file_path}`,
    UNNEST(ExtractMentions(content)) AS mention
  WHERE
   -- Filter: reducir el conjunto de datos
   -- Solo usa los que comienzan con arroba
    REGEXP_CONTAINS(content, r'@')
  GROUP BY
    mention
)

-- Main query: get top10
SELECT
  username,
  mention_count
FROM
  MentionCounts
WHERE
  -- No cuentes vacios
  LENGTH(username) > 0
ORDER BY
  mention_count DESC
LIMIT 10
"""


def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Q3 Time:
        El top 10 histórico de usuarios (username) más influyentes
        en función del conteo de las menciones (@) que registra cada uno de ellos
        Ejemplo de output:
            [("LATAM321", 387), ("LATAM_CHI", 129), ...]
        q3_time ejecuta la consulta Q3_TIME_QUERY que resuelve lo anterior
        con un enfoque eficiente en tiempo de ejecución.

    Args:
        file_path (str): project.dataset.table en bigquery
    Returns:
        List[Tuple[str, int]]: Una lista de tuplas que contienen el nombre de usuario
        y el conteo de menciones.
    Raises:
        GoogleCloudError: Si hay un error con la API de Google Cloud.
    """
    logger.info("Starting: q3_time")
    query = Q3_TIME_QUERY.format(file_path=file_path)

    try:
        query_job, client_execution_time = execute_query_with_benchmark(
            GCP_CLIENT, query, JOB_CONFIG
        )
        print_job_details(query_job, client_execution_time)
        results = [(row.username, row.mention_count) for row in query_job.result()]
        logger.info("Successful finish: q3_time")
        return results
    except GoogleCloudError as e:
        print(f"Error en Bigquery: {str(e)}")
        raise


if __name__ == "__main__":
    bq_file_path: str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    result: List[Tuple[str, int]] = q3_time(bq_file_path)
    print(result)
