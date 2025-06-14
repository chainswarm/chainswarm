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

            # Use generic asset parameter examples
            asset_param_example = "r.asset = $asset"

            return {
                "schema": schema[0]["schema"] if schema else "No schema available",
                "indexes": indexes,
                "vector_indexes": vector_indexes,
                "asset_support": {
                    "description": "Address nodes are asset-agnostic. Edges contain asset property to identify which asset was transferred."
                },
                "example_queries": [
                    "MATCH (a:Address {address: $address})-[r:TO {asset: $asset}]->(b:Address) RETURN a, r, b LIMIT 1000",
                    f"MATCH (a:Address {{address: $address}})-[r:TO]->(b:Address) WHERE {asset_param_example} RETURN a, r, b LIMIT 1000",
                    "MATCH path = (start:Address {address: $source_address})-[rels:TO*BFS 1..3]->(target:Address {address: $target_address}) WHERE ALL(r IN rels WHERE r.asset = $asset) RETURN path LIMIT 1000",
                    "MATCH (a:Address) WHERE a.address IN $addresses CALL path.expand(a, ['TO'],[],1, 3) YIELD result as path RETURN path LIMIT 1000",
                    "MATCH (a:Address)-[r:TO]->(b:Address) RETURN r.asset as asset, COUNT(r) as transfer_count, SUM(r.volume) as total_volume GROUP BY r.asset LIMIT 1000"
                ],
                "query_limits": {
                    "max_path_depth": 3,
                    "max_results": 1000,
                    "note": "All path queries are limited to maximum 3 hops and 1000 results to protect database performance"
                }
            }

    async def money_flow_query(self, query: str, assets: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Execute money flow query against Memgraph with asset support

        Args:
            query (str): The Cypher query to execute
            assets (List[str], optional): List of asset symbols for context

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
                            "error": "This query is not allowed in read-only mode. Please check the query and try again.",
                            "asset_context": {
                                "requested_assets": assets or ["Network native asset"],
                                "note": "Use asset properties to filter by specific assets"
                            }
                        }

            with self.graph_database.session() as session:
                result = session.run(query)
                json_result = result.data()
                
                # Add asset context to successful results
                if json_result is not None:
                    return {
                        "data": json_result,
                        "asset_context": {
                            "requested_assets": assets or ["Network native asset"],
                            "note": "Results may include asset properties on nodes and edges"
                        }
                    }
                else:
                    return {
                        "data": None,
                        "asset_context": {
                            "requested_assets": assets or ["Network native asset"],
                            "note": "No results found"
                        }
                    }
        except Exception as e:
            logger.error(f"Error querying graph database: {str(e)}")
            raise e