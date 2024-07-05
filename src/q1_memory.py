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
QUERY_PRIORITY: bigquery.QueryPriority = bigquery.QueryPriority.BATCH
USE_LEGACY_SQL: bool = False
GCP_CLIENT = bigquery.Client(project=PROJECT_ID)
RETRY_CONFIG = retry.Retry(deadline=30)

JOB_CONFIG = bigquery.QueryJobConfig(
    use_query_cache=USE_QUERY_CACHE,
    priority=QUERY_PRIORITY,
    use_legacy_sql=USE_LEGACY_SQL,
)

Q1_MEMORY_QUERY: str = """
-- CTE: contar tweets por fecha
WITH date_counts AS (
  SELECT
    -- Convertir fecha a Date
    DATE(date) AS tweet_date,
    -- Contar numero de tweets por fecha
    COUNT(*) AS tweet_count
  FROM
    `{file_path}`
  GROUP BY
    DATE(date)
),
-- CTE: seleccionar top10 fechas con mas tweets
top_10_dates AS (
  SELECT tweet_date, tweet_count
  FROM date_counts
  ORDER BY tweet_count DESC
  LIMIT 10
),
-- CTE: contar tweets por user y fecha dentro de top10
user_counts AS (
  SELECT
    DATE(date) AS tweet_date,
    user.username,
    -- Contar el numero de tweets por user en cada fecha
    COUNT(*) AS user_tweet_count
  FROM
    `{file_path}`
  WHERE DATE(date) IN (SELECT tweet_date FROM top_10_dates)
  GROUP BY
    DATE(date), user.username
)
-- Por cada fecha seleccionar la fecha y user con mas tweets
SELECT
  t.tweet_date,
  -- Seleccionar el user con mas tweets en cada fecha
  ARRAY_AGG(u.username ORDER BY u.user_tweet_count DESC LIMIT 1)[OFFSET(0)] AS top_user
FROM
  top_10_dates t
JOIN
  user_counts u
ON
  t.tweet_date = u.tweet_date
GROUP BY
  t.tweet_date, t.tweet_count
-- Ordenar resultado de forma descedente
ORDER BY
  t.tweet_count DESC
"""


def q1_memory(file_path: str) -> List[Tuple[date, str]]:
    """
    Q1 Memory:
        Las top 10 fechas donde hay más tweets
        Ejemplo de output:
            [(datetime.date(1999, 11, 15), "LATAM321"),
            (datetime.date(1999, 7, 15), "LATAM_CHI"), ...]
        q1_memory ejecuta la consulta Q1_MEMORY_QUERY que resuelve lo anterior
        con un enfoque eficiente en memoria

    Args:
        file_path (str): project.dataset.table en bigquery
    Returns:
        List[Tuple[date, str]]: Una lista de tuplas que contienen la fecha
                                y el usuario más activo para cada fecha.
    Raises:
        GoogleCloudError: Si hay un error con la API de Google Cloud.
    """
    logger.info("Starting: q1_memory")
    query = Q1_MEMORY_QUERY.format(file_path=file_path)

    try:
        query_job, client_execution_time = execute_query_with_benchmark(
            GCP_CLIENT, query, JOB_CONFIG
        )
        print_job_details(query_job, client_execution_time)
        results = [(row.tweet_date, row.top_user) for row in query_job.result()]
        logger.info("Successful finish: q1_memory")
        return results
    except GoogleCloudError as e:
        print(f"Error en Bigquery: {str(e)}")
        raise


if __name__ == "__main__":
    bq_file_path: str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    result: List[Tuple[date, str]] = q1_memory(bq_file_path)
    print(result)
