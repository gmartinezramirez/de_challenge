import logging
from datetime import datetime
from typing import List, Tuple

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    logger.info("Start Q1 Memory function")
    return []

if __name__ == "__main__":
    json_file_path = "data/farmers-protest-tweets-2021-2-4.json"
    q1_memory(json_file_path)
