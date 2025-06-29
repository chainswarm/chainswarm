import os
import sys
import signal
import threading
from loguru import logger
from .metrics import setup_metrics, get_metrics_registry, shutdown_metrics_servers, IndexerMetrics


def get_memgraph_connection_string(network: str):
    graph_db_url = os.getenv(
        f"{network.upper()}_MEMGRAPH_URL",
        f"bolt://localhost:17687"
    )

    graph_db_user = os.getenv(
        f"{network.upper()}_MEMGRAPH_USER",
        "mario"
    )

    graph_db_password = os.getenv(
        f"{network.upper()}_MEMGRAPH_PASSWORD",
        "Mario667!"
    )

    return graph_db_url, graph_db_user, graph_db_password

def get_neo4j_connection_string(network: str):
    graph_db_url = os.getenv(
        f"{network.upper()}_NEO4J_URL",
        f"bolt://localhost:7687"
    )

    graph_db_user = os.getenv(
        f"{network.upper()}_NEO4J_USER",
        "neo4j"
    )

    graph_db_password = os.getenv(
        f"{network.upper()}_NEO4J_PASSWORD",
        "neo4j"
    )

    return graph_db_url, graph_db_user, graph_db_password


def get_clickhouse_connection_string(network: str):
    connection_params = {
        "host": os.getenv(f"{network.upper()}_CLICKHOUSE_HOST", "localhost"),
        "port": os.getenv(f"{network.upper()}_CLICKHOUSE_PORT", "8123"),
        "database": os.getenv(f"{network.upper()}_CLICKHOUSE_DATABASE", f"{network}"),
        "user": os.getenv(f"{network.upper()}_CLICKHOUSE_USER", "default"),
        "password": os.getenv(f"{network.upper()}_CLICKHOUSE_PASSWORD", "changeit456$"),
        "max_execution_time": int(os.getenv(f"{network.upper()}_CLICKHOUSE_MAX_EXECUTION_TIME", "1800")),
        "max_query_size": int(os.getenv(f"{network.upper()}_CLICKHOUSE_MAX_QUERY_SIZE", "5000000")),
    }

    return connection_params


def create_clickhouse_database(connection_params):
    from clickhouse_connect import get_client
    client = get_client(
        host=connection_params['host'],
        port=int(connection_params['port']),
        username=connection_params['user'],
        password=connection_params['password'],
        database='default'
    )

    client.command(f"CREATE DATABASE IF NOT EXISTS {connection_params['database']}")


def setup_logger(service_name):
    def patch_record(record):
        record["extra"]["service"] = service_name
        return True

    # Get the absolute path to the project root directory
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    logs_dir = os.path.join(project_root, "logs")
    
    # Create logs directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)
    
    logger.remove()

    # File logger with JSON serialization for Loki ingestion
    logger.add(
        os.path.join(logs_dir, f"{service_name}.log"),
        rotation="500 MB",
        level="DEBUG",
        filter=patch_record,
        serialize=True  # Use loguru's built-in JSON serialization
    )

    # Console logger with human-readable format
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{extra[service]}</cyan> | <blue>{message}</blue>",
        level="DEBUG",
        filter=patch_record,
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )


terminate_event = threading.Event()


def shutdown_handler(signum, frame):
    logger.info("Shutdown signal received. Waiting for current processing to complete...")
    terminate_event.set()


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)
