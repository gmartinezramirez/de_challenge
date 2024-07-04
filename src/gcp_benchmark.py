import logging
import time
from typing import Tuple

from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def print_job_details(
    query_job: bigquery.QueryJob, client_execution_time: float
) -> None:
    """
    - Imprimir detalle de la ejecución del job en Big Query
    - Detalle referente a tiempos de ejecución, slot time usado y query plan
    - Recibe el client_execution_time con el objetivo de realizar cálculos de cuanto tiempo
    tomo la ejecución tanto en el client (máquina que ejecuta el código python como el server GCP)
    Args:
        query_job (bigquery.QueryJob):
        client_execution_time (float): El tiempo tomado desde el envío de la consulta hasta la recepción del resultado.
    """
    server_execution_time = (query_job.ended - query_job.started).total_seconds()
    time_delta = client_execution_time - server_execution_time

    logger.info("Bigquery Job Detail:")
    logger.info(f"Job ID: {query_job.job_id}")
    logger.info(f"Job Status: {query_job.state}")
    logger.info(f"Tiempo inicio en máquina GCP: {query_job.started}")
    logger.info(f"Tiempo fin en máquina GCP: {query_job.ended}")
    logger.info(
        f"Tiempo de ejecución total en servidor GCP: {server_execution_time} segundos"
    )
    logger.info(
        f"Tiempo de ejecución total en cliente (python): {client_execution_time:.2f} segundos"
    )
    logger.info(
        f"Delta de tiempo (costo de red, serialización, SO, etc.): {time_delta:.2f} segundos"
    )
    logger.info(f"Bytes procesados: {query_job.total_bytes_processed}")
    logger.info(f"Bytes facturados: {query_job.total_bytes_billed}")
    logger.info(f"Slot machine miliseconds: {query_job.slot_millis}")

    estimated_memory_usage = query_job.total_bytes_processed / (1024 * 1024)  # MB
    logger.info(
        f"Uso estimado de memoria en base a total bytes procesados: {estimated_memory_usage:.2f} MB"
    )

    if query_job.query_plan:
        logger.info("Query Plan:")
        for step in query_job.query_plan:
            logger.info(f"  Step {step.name}:")
            logger.info(f"    - Records read: {step.records_read}")
            logger.info(f"    - Records written: {step.records_written}")
            logger.info(f"    - Status: {step.status}")
    else:
        logger.info("Query Plan no disponible por uso de cache")


def execute_benchmark_query(
    client: bigquery.Client, query: str, use_cache: bool
) -> Tuple[bigquery.QueryJob, float]:
    """
    Ejecuta una query de BigQuery y devuelve el job con su tiempo de ejecución -total, en el server de GCP-.

    Args:
        client (bigquery.Client)
        query (str)
        use_cache (bool)

    Returns:
        Tuple[bigquery.QueryJob, float]: El job y tiempo de ejecución del lado del cliente python.
    """
    job_config = bigquery.QueryJobConfig(use_query_cache=use_cache)
    start_time = time.time()
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    end_time = time.time()

    client_execution_time = end_time - start_time
    logger.info(
        f"Tiempo de ejecución de la query (lado del cliente, python, hasta recibir la respuesta): {client_execution_time:.2f} segundos"
    )
    logger.info(f"Se uso query cache?: {use_cache}")

    return query_job, client_execution_time
