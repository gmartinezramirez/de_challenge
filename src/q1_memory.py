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


Q1_MEMORY_QUERY: str = """
-- Common Table Expression(CTE)
-- CTE 1: Contar tweets por fecha
WITH date_counts AS (
  SELECT
    DATE(date) AS tweet_date,
    COUNT(*) AS tweet_count
  FROM
    `{project}.{dataset}.{table}`
  GROUP BY
    DATE(date)
),
-- CTE 2: Seleccionar las 10 fechas con más tweets
top_10_dates AS (
  SELECT
    tweet_date,
    tweet_count
  FROM
    date_counts
  ORDER BY
    tweet_count DESC
  LIMIT 10
),
-- CTE 3: Contar tweets por usuario y fecha
user_counts AS (
  SELECT
    DATE(date) AS tweet_date,
    user.username,
    COUNT(*) AS user_tweet_count
  FROM
    `{project}.{dataset}.{table}`
  GROUP BY
    DATE(date),
    user.username
),
-- CTE 4: Hacer ranking de usuarios por número de tweets en cada fecha
ranked_users AS (
  SELECT
    tweet_date,
    username,
    user_tweet_count,
    ROW_NUMBER() OVER (PARTITION BY tweet_date ORDER BY user_tweet_count DESC) AS rank
  FROM
    user_counts
)
-- Query principal: Unir las top 10 fechas con los usuarios más activos
-- Output tweet_date, top_user (username)
SELECT
  t.tweet_date,
  r.username AS top_user
FROM
  top_10_dates t
JOIN
  ranked_users r
ON
  t.tweet_date = r.tweet_date
WHERE
  r.rank = 1
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
        file_path (str): No se usa en esta implementación
        se mantiene por consistencia con la firma de la función.
    Returns:
        List[Tuple[date, str]]: Una lista de tuplas que contienen la fecha
                                y el usuario más activo para cada fecha.
    Raises:
        GoogleCloudError: Si hay un error con la API de Google Cloud.
    """
    logger.info("Starting: q1_memory")
    query = Q1_MEMORY_QUERY.format(
        project=PROJECT_ID, dataset=DATASET_ID, table=TABLE_NAME
    )

    try:
        query_job, client_execution_time = execute_query_with_benchmark(
            GCP_CLIENT, query, JOB_CONFIG
        )
        print_job_details(query_job, client_execution_time)
        results = [(row.tweet_date, row.top_user) for row in query_job.result()]
        logger.info("Sucessful finish: q1_memory")
        return results
    except GoogleCloudError as e:
        print(f"Error en Bigquery: {str(e)}")
        raise


if __name__ == "__main__":
    result = q1_memory("something")
    print(result)
