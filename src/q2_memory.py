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

# Unicode ranges
EMOTICONS = r"\x{1F600}-\x{1F64F}"
MISC_SYMBOLS_PICTOGRAPHS = r"\x{1F300}-\x{1F5FF}"
TRANSPORT_MAP_SYMBOLS = r"\x{1F680}-\x{1F6FF}"
FLAGS = r"\x{1F1E6}-\x{1F1FF}"
MISC_SYMBOLS = r"\x{2600}-\x{26FF}"
DINGBATS = r"\x{2700}-\x{27BF}"
SUPPLEMENTAL_SYMBOLS_PICTOGRAPHS = r"\x{1F900}-\x{1F9FF}"
SYMBOLS_PICTOGRAPHS_EXTENDED = r"\x{1FA70}-\x{1FAFF}"

UNICODE_RANGES = (
    f"[{EMOTICONS}"
    f"{MISC_SYMBOLS_PICTOGRAPHS}"
    f"{TRANSPORT_MAP_SYMBOLS}"
    f"{FLAGS}"
    f"{MISC_SYMBOLS}"
    f"{DINGBATS}"
    f"{SUPPLEMENTAL_SYMBOLS_PICTOGRAPHS}"
    f"{SYMBOLS_PICTOGRAPHS_EXTENDED}]"
)

Q2_MEMORY_QUERY: str = rf"""
-- Funcion: extraer emojis unicos de un string
CREATE TEMP FUNCTION ExtractEmoji(content STRING) AS (
  -- usar ARRAY_AGG con DISTINCT para eliminar duplicados de inmediato
  (SELECT ARRAY_AGG(DISTINCT char IGNORE NULLS)
   -- Separa content en char
   FROM UNNEST(SPLIT(content, '')) AS char
   -- Filtra caracteres que hacen match con rangos unicode
   WHERE REGEXP_CONTAINS(char, r'{UNICODE_RANGES}'))
);

-- Main Query
WITH emoji_counts AS (
  -- Extrae y cuenta emojis en una pasada
  SELECT
    emoji,
    COUNT(*) as count
  FROM
     `{{file_path}}`,
    -- Usa la funcion para extraer emojis y hace unnest de los resultados
    UNNEST(ExtractEmoji(content)) as emoji
  -- Group por emoji unico
  GROUP BY
    emoji
)
-- Select the top 10 emojis
SELECT emoji, count
FROM (
  SELECT
    emoji,
    count,
    -- Asigna un ranking cada emoji basado en su count y desc
    RANK() OVER (ORDER BY count DESC) as rank
  FROM emoji_counts
)
WHERE rank <= 10
ORDER BY count DESC
"""


def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Q2 Memory:
        Los top 10 emojis más usados con su respectivo conteo
        Ejemplo de output:
            [("✈️", 6856), ("❤️", 5876), ...]
        q2_memory ejecuta la consulta Q2_MEMORY_QUERY que resuelve lo anterior
        con un enfoque eficiente en tiempo de ejecución

    Args:
        file_path (str): project.dataset.table en bigquery
    Returns:
        List[Tuple[str, int]]: Una lista de tuplas que contienen la fecha
                                y el usuario más activo para cada fecha.
    Raises:
        GoogleCloudError: Si hay un error con la API de Google Cloud.
    """
    logger.info("Starting: q2_memory")
    query = Q2_MEMORY_QUERY.replace("{file_path}", file_path)

    try:
        query_job, client_execution_time = execute_query_with_benchmark(
            GCP_CLIENT, query, JOB_CONFIG
        )
        print_job_details(query_job, client_execution_time)
        results = [(row.emoji, row.count) for row in query_job.result()]
        logger.info("Successful finish: q2_memory")
        return results
    except GoogleCloudError as e:
        print(f"Error en Bigquery: {str(e)}")
        raise


if __name__ == "__main__":
    bq_file_path: str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    result: List[Tuple[str, int]] = q2_memory(bq_file_path)
    print(result)
