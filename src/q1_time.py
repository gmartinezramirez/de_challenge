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


Q1_TIME_QUERY = """
-- Common Table Expression(CTE)
-- CTE 1: Contar tweets por fecha y usuario de forma paralela
WITH date_user_counts AS (
  SELECT
    DATE(date) AS tweet_date,
    user.username,
    COUNT(*) AS tweet_count
  FROM
    `{project}.{dataset}.{table}`
  GROUP BY
    DATE(date), user.username
),

-- CTE 2: Calcular total por fecha y hace ranking de usuarios
date_totals_and_top_users AS (
  SELECT
    tweet_date,
    username,
    tweet_count AS user_tweet_count,
    SUM(tweet_count) OVER (PARTITION BY tweet_date) AS total_date_count,
    ROW_NUMBER() OVER (PARTITION BY tweet_date ORDER BY tweet_count DESC) AS user_rank
  FROM
    date_user_counts
)

-- Main Query: Seleccionar top 10 fechas y sus usuarios más activos
SELECT
  tweet_date,
  username AS top_user
FROM
  date_totals_and_top_users
WHERE
  user_rank = 1
  AND tweet_date IN (
    SELECT tweet_date
    FROM (
      SELECT tweet_date, total_date_count,
             ROW_NUMBER() OVER (ORDER BY total_date_count DESC) AS date_rank
      FROM date_totals_and_top_users
      WHERE user_rank = 1
    )
    WHERE date_rank <= 10
  )
ORDER BY
  total_date_count DESC
LIMIT 10
"""


def q1_time(file_path: str) -> List[Tuple[date, str]]:
    """
    Q1 Time:
        Las top 10 fechas donde hay más tweets
        Ejemplo de output:
            [(datetime.date(1999, 11, 15), "LATAM321"),
            (datetime.date(1999, 7, 15), "LATAM_CHI"), ...]
        q1_time ejecuta la consulta Q1_TIME_QUERY que resuelve lo anterior
        con un enfoque eficiente en tiempo de ejecución

    Args:
        file_path (str): No se usa en esta implementación
        se mantiene por consistencia con la firma de la función.
    Returns:
        List[Tuple[date, str]]: Una lista de tuplas que contienen la fecha
                                y el usuario más activo para cada fecha.
    Raises:
        GoogleCloudError: Si hay un error con la API de Google Cloud.
    """
    logger.info("Starting: q1_time")
    query = Q1_TIME_QUERY.format(
        project=PROJECT_ID, dataset=DATASET_ID, table=TABLE_NAME
    )

    try:
        query_job, client_execution_time = execute_query_with_benchmark(
            GCP_CLIENT, query, JOB_CONFIG
        )
        print_job_details(query_job, client_execution_time)
        results = [(row.tweet_date, row.top_user) for row in query_job.result()]
        logger.info("Sucessful finish: q1_time")
        return results
    except GoogleCloudError as e:
        print(f"Error en Bigquery: {str(e)}")
        raise


if __name__ == "__main__":
    result = q1_time("something")
    print(result)
