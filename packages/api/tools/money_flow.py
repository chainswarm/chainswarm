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

    def get_money_flow_schema(self, assets: Optional[List[str]] = None) -> Dict[str, Any]:
        """Get schema information from Memgraph with asset support

        Args:
            assets: List of asset symbols to include in schema info

        Returns:
            Dict containing schema information including node labels, indexes, vector indexes, asset properties, and example queries
        """

        with self.graph_database.session() as session:
            schema = session.run("CALL llm_util.schema('raw') YIELD schema RETURN schema;").data()
            indexes = session.run("SHOW INDEX INFO").data()
            vector_indexes = session.run("CALL vector_search.show_index_info() YIELD * RETURN *;").data()

            # Default asset examples
            asset_examples = assets if assets else ["TOR"]
            # Use proper parameterization for asset filters in example queries
            if len(asset_examples) == 1:
                asset_filter = f"r.asset = '{asset_examples[0]}'"
                asset_param_example = f"r.asset = $asset"
            else:
                asset_filter = f"r.asset IN {asset_examples}"
                asset_param_example = f"r.asset IN $assets"

            return {
                "schema": schema[0]["schema"] if schema else "No schema available",
                "indexes": indexes,
                "vector_indexes": vector_indexes,
                "asset_support": {
                    "description": "Money flow graph includes asset properties on edges only. Address nodes are identified by address only.",
                    "asset_properties": {
                        "nodes": [],
                        "edges": ["asset", "id"]
                    },
                    "edge_naming": "Edge IDs are suffixed with asset symbol: from-{from_address}-to-{to_address}-{asset}",
                    "requested_assets": assets or ["Network native asset"]
                },
                "example_queries": [
                    f"MATCH (a:Address {{address: $address}})-[r:TO {{asset: $asset}}]->(b:Address) RETURN a, r, b LIMIT 10",
                    f"MATCH (a:Address {{address: $address}})-[r:TO]->(b:Address) WHERE {asset_param_example} RETURN a, r, b LIMIT 10",
                    f"MATCH path = (start:Address {{address: $source_address}})-[rels:TO*BFS]->(target:Address {{address: $target_address}}) WHERE ALL(r IN rels WHERE r.asset = $asset) RETURN path",
                    f"MATCH (a:Address) WHERE a.address IN $addresses CALL path.expand(a, ['TO'],[],0, 2) YIELD result as path RETURN path",
                    "MATCH (a:Address)-[r:TO]->(b:Address) RETURN r.asset as asset, COUNT(r) as transfer_count, SUM(r.volume) as total_volume GROUP BY r.asset"
                ]
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