from typing import Dict, Any, List
from neo4j import GraphDatabase, Driver
from neo4j.exceptions import ClientError
from packages.indexers.base import get_memgraph_connection_string, get_neo4j_connection_string


def get_memgraph_driver(network):
    """
    Create and return a Memgraph driver instance.

    Returns:
        GraphDatabase.driver: A Memgraph driver instance configured with the connection details.
    """
    graph_db_url, graph_db_user, graph_db_password = get_memgraph_connection_string(network)
    return GraphDatabase.driver(
        graph_db_url,
        auth=(graph_db_user, graph_db_password),
        max_connection_lifetime=3600,
        connection_acquisition_timeout=60
    )


def get_neo4j_driver(network: str) -> Driver:
    """Create a Neo4j driver for the specified network

    Returns:
        Neo4j driver instance
    """
    graph_db_url, graph_db_user, graph_db_password = get_neo4j_connection_string(network)

    return GraphDatabase.driver(
        graph_db_url,
        auth=(graph_db_user, graph_db_password),
        max_connection_lifetime=3600,
        connection_acquisition_timeout=60
    )


def execute_memgraph_query(driver: Driver, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """Execute a Cypher query against Memgraph

    Args:
        driver: Memgraph driver instance
        query: Cypher query to execute
        params: Query parameters

    Returns:
        List of results as dictionaries
    """
    with driver.session() as session:
        result = session.run(query, params or {})
        return result.data()

def execute_neo4j_query(driver: Driver, query: str, params: Dict[str, Any] = None) -> bool:
    """Execute a query with Neo4j to validate it's read-only

    Args:
        driver: Neo4j driver instance
        query: Cypher query to execute
        params: Query parameters

    Returns:
        True if the query is valid and read-only, False otherwise
    """
    try:
        # Use read transaction to ensure query is read-only
        with driver.session() as session:
            result = session.execute_read(
                lambda tx: tx.run(query, params or {}).data()
            )
            return True
    except ClientError as e:
        # Check if this is a write operation in read-only transaction
        if "write operations are not allowed" in str(e).lower():
            raise ValueError("Write operations are not allowed. Query must be read-only.")
        # If it's another Neo4j-specific error, we'll try with Memgraph
        return False