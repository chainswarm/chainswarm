from typing import Any, Dict, List, Optional
from loguru import logger
from neo4j import Driver
from packages.indexers.substrate import get_network_asset


class MoneyFlowTool:
    def __init__(self, graph_database: Driver, guard_graph_database: Driver):
        self.graph_database = graph_database
        self.guard_graph_database = guard_graph_database

    def close(self):
        if self.graph_database:
            self.graph_database.close()
        if self.guard_graph_database:
            self.guard_graph_database.close()

    def get_money_flow_schema(self) -> Dict[str, Any]:
        """Get schema information from Memgraph with asset support

        Returns:
            Dict containing schema information including node labels, indexes, vector indexes, asset properties, and example queries
        """

        with self.graph_database.session() as session:
            schema = session.run("CALL llm_util.schema('raw') YIELD schema RETURN schema;").data()
            indexes = session.run("SHOW INDEX INFO").data()
            vector_indexes = session.run("CALL vector_search.show_index_info() YIELD * RETURN *;").data()

            return {
                "schema": schema[0]["schema"] if schema else "No schema available",
                "indexes": indexes,
                "vector_indexes": vector_indexes,
                "hints": [
                    "No list comprehensions: [rel IN rels | rel.volume] is not supported",
                    "No negative indexing: volumes[-1] doesn't work",
                    "No length() function: Use size() instead",
                    "Limited variable-length path processing: Better to use explicit UNION queries"
                    ],
                "example_queries": {
                    "finds the shortest path between two addresses with asset support": [
                        "MATCH path = (start:Address {address: \"5FmxDRxcS4WDUsLywoCwBPFKrXS411gMN84KwvPDQ3YMX6fP\"})-[rels:TO*BFS]-(target:Address {address: \"5DDXwRsgvdfukGZbq2o27n43qyDaAnZ6rsfeckGxnaQ1ih2D\"}) WHERE ALL(rel IN rels WHERE rel.asset = \"TOR\") AND size(path) > 0 RETURN path ORDER BY size(rels)"
                    ]
                },
            }

    async def money_flow_query(self, query: str) -> Dict[str, Any]:
        """
        Execute money flow query against Memgraph with asset support

        Args:
            query (str): The Cypher query to execute

        Returns:
            Dict containing query results with asset metadata
        """

        try:
            with self.guard_graph_database.session(default_access_mode="READ") as session:
                try:
                    session.run(query).data()
                except Exception as e:
                    # Check if this is a write operation in read-only transaction
                    if "write operations are not allowed" in str(e).lower():
                        return {
                            "error": "This query is not allowed in read-only mode. Please check the query and try again."
                        }

            with self.graph_database.session() as session:
                result = session.run(query)
                json_result = result.data()
                
                if json_result is not None:
                    logger.info(f"Query executed successfully: {query}")
                    return {
                        "data": json_result,
                    }
                else:
                    return {
                        "data": None,
                    }
        except Exception as e:
            error_message = f"Error querying graph database: {query}; {str(e)}"
            logger.exception(error_message)
            raise ValueError(f"Error querying graph database: {query}; {str(e)}") from e