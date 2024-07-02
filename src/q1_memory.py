import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TweetUser:
    """Representa un Twitter user con username."""

    username: str


@dataclass(frozen=True)
class TweetData:
    """Representa un tweet con una fecha en str y user information como TweetUser."""

    date: str
    user: TweetUser


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """Procesa la info de un filepath y retorna una lista de tuplas de fechas y strings.
    Optimizando uso de memoria

    Args:
        file_path (str): El filepath del archivo a procesar

    Returns:
        List[Tuple[datetime.date, str]]: Lista de tuplas de fechas y strings.
    """
    logger.info("Start Q1 Memory function")
    return []


if __name__ == "__main__":
    JSON_FILE_PATH = "data/farmers-protest-tweets-2021-2-4.json"
    q1_memory(JSON_FILE_PATH)
