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
        client_execution_time (float): El tiempo tomado desde el envío de la consulta hasta
                                       recibir el resultado.
    """
    server_execution_time = (query_job.ended - query_job.started).total_seconds()
    time_delta = client_execution_time - server_execution_time
    logger.info("Bigquery Job Detail:")
    logger.info("Job ID: %s", query_job.job_id)
    logger.info("Job Status: %s", query_job.state)
    logger.info("Tiempo inicio en máquina GCP: %s", query_job.started)
    logger.info("Tiempo fin en máquina GCP: %s", query_job.ended)
    logger.info(
        "Tiempo de ejecución total en servidor GCP: %s segundos", server_execution_time
    )
    logger.info(
        "Tiempo de ejecución total en cliente (python): %.2f segundos",
        client_execution_time,
    )
    logger.info(
        "Delta de tiempo (costo de red, serialización, SO, etc.): %.2f segundos",
        time_delta,
    )
    logger.info("Bytes procesados: %s", query_job.total_bytes_processed)
    logger.info("Bytes facturados: %s", query_job.total_bytes_billed)
    logger.info("Slot machine miliseconds: %s", query_job.slot_millis)
    estimated_memory_usage = query_job.total_bytes_processed / (1024 * 1024)  # MB
    logger.info(
        "Uso estimado de memoria en base a total bytes procesados: %.2f MB",
        estimated_memory_usage,
    )
    if query_job.query_plan:
        logger.info("Query Plan:")
        for step in query_job.query_plan:
            logger.info("  Step %s:", step.name)
            logger.info("    - Records read: %s", step.records_read)
            logger.info("    - Records written: %s", step.records_written)
            logger.info("    - Status: %s", step.status)
    else:
        logger.info("Query Plan no disponible por uso de cache")


def execute_query_with_benchmark(
    client: bigquery.Client, query: str, use_cache: bool
) -> Tuple[bigquery.QueryJob, float]:
    """
    Ejecuta una query de BigQuery y devuelve el job con su tiempo de ejecución
    -total, en el server de GCP-
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
    query_job.result()  # Esperar a que el job termine
    end_time = time.time()
    client_execution_time = end_time - start_time
    logger.info(
        "Tiempo de ejecución de la query (lado del cliente, python, "
        "hasta recibir la respuesta): %.2f segundos",
        client_execution_time,
    )
    logger.info("Se uso query cache?: %s", use_cache)
    return query_job, client_execution_time
