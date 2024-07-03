import logging
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import DefaultDict, List, Tuple  # type: ignore

import pandas as pd  # type: ignore

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_top_users(
    top_dates: List[Tuple[date, int]],
    date_user_counts: DefaultDict[date, DefaultDict[str, int]],
) -> List[Tuple[date, str]]:
    """Obtiene el usuario más activo para cada una de las top 10 fechas."""
    return [
        (tweet_date, max(date_user_counts[tweet_date].items(), key=lambda x: x[1])[0])
        for tweet_date, _ in top_dates
    ]


def get_top_10_dates(date_counts: DefaultDict[date, int]) -> List[Tuple[date, int]]:
    """Obtiene las top 10 fechas con más tweets."""
    return sorted(date_counts.items(), key=lambda x: x[1], reverse=True)[:10]


def process_chunk(
    chunk: pd.DataFrame,
) -> Tuple[DefaultDict[date, int], DefaultDict[date, DefaultDict[str, int]]]:
    """Procesa un chunk de datos y retorna los conteos de fechas y usuarios."""
    date_counts: DefaultDict[date, int] = defaultdict(int)
    date_user_counts: DefaultDict[date, DefaultDict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    chunk["date"] = pd.to_datetime(chunk["date"]).dt.date
    for _, row in chunk.iterrows():
        tweet_date: date = row["date"]
        username: str = row["user"]["username"]
        date_counts[tweet_date] += 1
        date_user_counts[tweet_date][username] += 1
    return date_counts, date_user_counts


def get_total_counts_batch(
    file_path: str, chunk_size: int
) -> Tuple[DefaultDict[date, int], DefaultDict[date, DefaultDict[str, int]]]:
    """Obtiene los conteos totales de fechas y usuarios del archivo."""
    total_date_counts: DefaultDict[date, int] = defaultdict(int)
    total_date_user_counts: DefaultDict[date, DefaultDict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    total_rows = 0

    # Obtener el número total de chunks
    with pd.read_json(file_path, lines=True, chunksize=chunk_size) as reader:
        logger.info(f"Calculando el total de chunks a procesar")
        total_chunks = sum(1 for _ in reader)
    logger.info(f"Total de chunks a procesar: {total_chunks}")

    chunks = pd.read_json(file_path, lines=True, chunksize=chunk_size)
    for i, chunk in enumerate(chunks):
        chunk_rows = len(chunk)
        total_rows += chunk_rows
        date_counts, date_user_counts = process_chunk(chunk)
        for tweet_date, count in date_counts.items():
            total_date_counts[tweet_date] += count
        for tweet_date, user_counts in date_user_counts.items():
            for user, count in user_counts.items():
                total_date_user_counts[tweet_date][user] += count
        logger.info(
            f"Procesando chunk {i+1}/{total_chunks} - Filas en este chunk: {chunk_rows}"
        )

    logger.info(f"Total de filas procesadas: {total_rows}")
    return total_date_counts, total_date_user_counts


def q1_memory(file_path: str) -> List[Tuple[date, str]]:
    """
    Q1: Las top 10 fechas donde hay más tweets.
    file_path es un input de dataset json de 117407 registros (filas) de peso 407,7 mb
    (verificado por macOS)
    Mencionar el usuario (username) que más publicaciones tiene por cada uno de esos días.
    Ejemplo de retorno:
        [(datetime.date(1999, 11, 15), "LATAM321"), (datetime.date(1999, 7, 15), "LATAM_CHI"), ...]
    """
    logger.info("Iniciando: Q1 - Optimizado para consumo de memoria")
    if not Path(file_path).exists():
        raise FileNotFoundError(f"El archivo {file_path} no existe")
    try:
        # Setup variables
        BATCH_SIZE = 10000

        # Paso 1: Obtener conteos totales en batch
        total_date_counts, total_date_user_counts = get_total_counts_batch(
            file_path, BATCH_SIZE
        )
        # Paso 2: Obtener top 10 fechas
        top_dates = get_top_10_dates(total_date_counts)
        # Paso 3: Obtener usuarios más activos para las top 10 fechas
        top_results = get_top_users(top_dates, total_date_user_counts)
        logger.info("Procesamiento de Q1 Memory completado con éxito")
        return top_results
        logger.info("Procesamiento de Q1 completado con éxito")
        return []
    except Exception as e:
        logger.error("Error en el pipeline principal: %s", str(e))
        raise


if __name__ == "__main__":
    # Abrir archivo json
    import os

    # Using sample file (only 50 registers)
    # JSON_FILE_PATH = os.path.join(os.path.dirname(__file__), "sample.json")
    # Using real file
    JSON_FILE_PATH = os.path.join(
        os.path.dirname(__file__), "farmers-protest-tweets-2021-2-4.json"
    )

    # Imprime información de depuración
    print("Carpeta actual:", os.getcwd())
    print("filepath:", JSON_FILE_PATH)

    result = q1_memory(JSON_FILE_PATH)
    print(result)
