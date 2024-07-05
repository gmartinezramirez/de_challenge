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

# Rangos de Unicode
# Referencia: https://www.unicode.org/charts/
EMOTICONS = r"\x{1F600}-\x{1F64F}"
MISC_SYMBOLS_PICTOGRAPHS = r"\x{1F300}-\x{1F5FF}"
TRANSPORT_MAP_SYMBOLS = r"\x{1F680}-\x{1F6FF}"
SUPPLEMENTAL_SYMBOLS_PICTOGRAPHS = r"\x{1F900}-\x{1F9FF}"
SYMBOLS_PICTOGRAPHS_EXTENDED = r"\x{1FA70}-\x{1FAFF}"
FLAGS = r"\x{1F1E6}-\x{1F1FF}"
MISC_SYMBOLS = r"\x{2600}-\x{26FF}"
DINGBATS = r"\x{2700}-\x{27BF}"
GEOMETRIC_SHAPES = r"\x{25A0}-\x{25FF}"
GEOMETRIC_SHAPES_EXTENDED = r"\x{1F780}-\x{1F7FF}"
SUPPLEMENTAL_ARROWS_C = r"\x{1F800}-\x{1F8FF}"
ENCLOSED_ALPHANUMERIC_SUPPLEMENT = r"\x{1F100}-\x{1F1FF}"
ENCLOSED_IDEOGRAPHIC_SUPPLEMENT = r"\x{1F200}-\x{1F2FF}"
SYMBOLS_ARROWS = r"\x{2B00}-\x{2BFF}"
ORNAMENTAL_DINGBATS = r"\x{1F650}-\x{1F67F}"
PLAYING_CARDS = r"\x{1F0A0}-\x{1F0FF}"
ALCHEMICAL_SYMBOLS = r"\x{1F700}-\x{1F77F}"
CHESS_SYMBOLS = r"\x{1FA00}-\x{1FA6F}"
SKIN_TONE_MODIFIERS = r"\x{1F3FB}-\x{1F3FF}"
ZERO_WIDTH_JOINER = r"\x{200D}"
GENDER_MODIFIERS = r"\x{2640}\x{2642}"

# Unir todos los rangos en un solo string
UNICODE_RANGES: str = "".join(
    [
        EMOTICONS,
        MISC_SYMBOLS_PICTOGRAPHS,
        TRANSPORT_MAP_SYMBOLS,
        SUPPLEMENTAL_SYMBOLS_PICTOGRAPHS,
        SYMBOLS_PICTOGRAPHS_EXTENDED,
        FLAGS,
        MISC_SYMBOLS,
        DINGBATS,
        GEOMETRIC_SHAPES,
        GEOMETRIC_SHAPES_EXTENDED,
        SUPPLEMENTAL_ARROWS_C,
        ENCLOSED_ALPHANUMERIC_SUPPLEMENT,
        ENCLOSED_IDEOGRAPHIC_SUPPLEMENT,
        SYMBOLS_ARROWS,
        ORNAMENTAL_DINGBATS,
        PLAYING_CARDS,
        ALCHEMICAL_SYMBOLS,
        CHESS_SYMBOLS,
        SKIN_TONE_MODIFIERS,
        ZERO_WIDTH_JOINER,
        GENDER_MODIFIERS,
    ]
)

Q2_MEMORY_QUERY: str = """
-- Funcion: extraer emojis unicos de un string
CREATE TEMP FUNCTION ExtractEmoji(content STRING) AS (
  -- Usar ARRAY_AGG con DISTINCT para eliminar duplicados inmediatamente
  (SELECT ARRAY_AGG(DISTINCT char IGNORE NULLS)
   -- Divide el content como char
   FROM UNNEST(SPLIT(content, '')) AS char
   -- Filtra caracteres que coincide con los rangos unicode
   WHERE REGEXP_CONTAINS(char, r'[{UNICODE_RANGES}]'))
);

-- Main Query
WITH emoji_counts AS (
  -- Extraer y contar emojis en una sola ejecución
  SELECT
    emoji,
    COUNT(*) as count
  FROM
    `{file_path}`,
    -- Aplica la funcion para extraer emojis y unnest los resultados
    UNNEST(ExtractEmoji(content)) as emoji
  -- Agrupa por cada emoji unico
  GROUP BY
    emoji
)
-- Seleccionar los top 10 emojis
SELECT emoji, count
FROM (
  SELECT
    emoji,
    count,
    -- Asigna un ranking a cada emoji en base a su conteo descendente
    RANK() OVER (ORDER BY count DESC) as rank
  FROM emoji_counts
)
WHERE rank <= 10
ORDER BY count DESC
"""


def q2_memory(file_path: str) -> List[Tuple[date, str]]:
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
        List[Tuple[date, str]]: Una lista de tuplas que contienen la fecha
                                y el usuario más activo para cada fecha.
    Raises:
        GoogleCloudError: Si hay un error con la API de Google Cloud.
    """
    logger.info("Starting: q2_memory")
    query = Q2_MEMORY_QUERY.replace("{file_path}", file_path).replace(
        "{UNICODE_RANGES}", UNICODE_RANGES
    )

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
    result: List[Tuple[date, str]] = q2_memory(bq_file_path)
    print(result)
