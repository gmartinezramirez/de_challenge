from datetime import date
from typing import List, Tuple

from google.api_core import retry
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from gcp_benchmark import execute_query_with_benchmark, print_job_details

USE_QUERY_CACHE: bool = (
    False  # True: activa query caching, False: la query no usa cache
)
PROJECT_ID: str = "de-challenge-gm"
DATASET_ID: str = "tweets"
TABLE_NAME: str = "farmers-protest-tweets"
client = bigquery.Client(project=PROJECT_ID)
retry_config = retry.Retry(deadline=30)

Q1_MEMORY_QUERY: str = """
WITH date_counts AS (
  SELECT
    DATE(date) AS tweet_date,
    COUNT(*) AS tweet_count
  FROM
    `{project}.{dataset}.{table}`
  GROUP BY
    DATE(date)
),
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
ranked_users AS (
  SELECT
    tweet_date,
    username,
    user_tweet_count,
    ROW_NUMBER() OVER (PARTITION BY tweet_date ORDER BY user_tweet_count DESC) AS rank
  FROM
    user_counts
)
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
    Ejecuta la consulta de BigQuery eficiente en memoria y devuelve los resultados.
    Args:
        file_path (str): No se usa en esta implementación
        se mantiene por consistencia con la firma de la función.
    Returns:
        List[Tuple[date, str]]: Una lista de tuplas que contienen la fecha
                                y el usuario más activo para cada fecha.
    Raises:
        GoogleCloudError: Si hay un error con la API de Google Cloud.
    """
    query = Q1_MEMORY_QUERY.format(
        project=PROJECT_ID, dataset=DATASET_ID, table=TABLE_NAME
    )
    try:
        query_job, client_execution_time = execute_query_with_benchmark(
            client, query, USE_QUERY_CACHE
        )
        print_job_details(query_job, client_execution_time)
        results = [(row.tweet_date, row.top_user) for row in query_job.result()]
        return results
    except GoogleCloudError as e:
        print(f"Error en Bigquery: {str(e)}")
        raise


if __name__ == "__main__":
    result = q1_memory("something")
    print(result)
