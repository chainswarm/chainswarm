from typing import Any, Dict, List, Optional
from loguru import logger
from neo4j import Driver
from packages.indexers.substrate import get_network_asset
from enum import Enum


class Direction(str, Enum):
    in_ = 'in'
    out_ = 'out'
    all_ = 'all'


class MoneyFlowTool:
    def __init__(self, graph_database: Driver, guard_graph_database: Driver):
        self.graph_database = graph_database
        self.guard_graph_database = guard_graph_database

    def close(self):
        if self.graph_database:
            self.graph_database.close()
        if self.guard_graph_database:
            self.guard_graph_database.close()

    def schema(self) -> Dict[str, Any]:
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
            }

    def query(self, query: str) -> Dict[str, Any]:
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

    def shortest_path(self, source_address: str, target_address: str, assets: List[str] = None):
        """Get shortest path between two addresses with optional asset filtering
        
        Args:
            source_address: Source address
            target_address: Target address
            assets: Optional list of assets to filter by
            
        Returns:
            Raw query results containing nodes and edges
        """
        # Build asset filter for relationships
        params = {
            'source_address': source_address,
            'target_address': target_address
        }
        
        # Build asset filter conditions
        asset_filter_conditions = ["size(path) > 0"]
        if assets and assets != ["all"]:
            # Use parameterized queries to avoid injection and syntax errors
            asset_conditions = " OR ".join([f"rel.asset = $asset_{i}" for i, _ in enumerate(assets)])
            asset_filter_conditions.append(f"ALL(rel IN rels WHERE {asset_conditions})")
            # Add asset parameters
            for i, asset in enumerate(assets):
                params[f'asset_{i}'] = asset

        where_clause = " AND ".join(asset_filter_conditions)

        query = f"""
                MATCH path = (start:Address {{address: $source_address}})-[rels:TO*BFS]-(target:Address {{address: $target_address}})
                WHERE {where_clause}
                RETURN path
            """

        try:
            with self.graph_database.session() as session:
                result = session.run(query, params)
                return result.data()
        except Exception as e:
            logger.error(f"Error querying graph database: {str(e)}")
            return None

    def explore_address_connections(self, addresses: List[str],
                      depth_level: int,
                      direction: Direction,
                      assets: List[str] = None):
        """Explore money flows from addresses with depth and direction control
        
        Args:
            addresses: List of wallet addresses to start the exploration from
            depth_level: Number of hops to explore from the starting addresses
            direction: Direction of the relationships to follow
            assets: Optional list of assets to filter by
            
        Returns:
            Raw query results containing nodes and edges
        """
        to_relation = 'TO'
        if direction.value == Direction.in_:
            to_relation = '<TO'
        elif direction.value == Direction.out_:
            to_relation = 'TO>'

        # Build asset filter for relationships
        params = {'addresses': addresses}
        
        # Build asset filter conditions
        asset_filter_conditions = ["size(path) > 0"]
        if assets and assets != ["all"]:
            # Use parameterized queries to avoid injection and syntax errors
            asset_conditions = " OR ".join([f"rel.asset = $asset_{i}" for i, _ in enumerate(assets)])
            asset_filter_conditions.append(f"ALL(rel IN relationships(path) WHERE {asset_conditions})")
            # Add asset parameters
            for i, asset in enumerate(assets):
                params[f'asset_{i}'] = asset

        where_clause = " AND ".join(asset_filter_conditions)

        query = f"""
            MATCH (a:Address) WHERE a.address IN $addresses
            CALL path.expand(a, ["{to_relation}"], [], 0, {depth_level}) YIELD result as path
            WITH path
            WHERE {where_clause}
            RETURN path
        """

        try:
            with self.graph_database.session() as session:
                result = session.run(query, params)
                return result.data()
        except Exception as e:
            logger.error(f"Error querying graph database: {str(e)}")
            return None