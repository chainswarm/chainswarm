from typing import Any, Dict, Optional, List, Tuple
from loguru import logger
from neo4j import Driver, GraphDatabase
from enum import Enum


class Direction(str, Enum):
    in_ = 'in'
    out_ = 'out'
    all_ = 'all'


class MoneyFlowService:
    def __init__(self, graph_database: Driver):
        self.graph_database = graph_database

    def close(self):
        if self.graph_database:
            self.graph_database.close()

    def get_money_flow_by_path_shortest(self, source_address: str, target_address: str, assets: List[str] = None):
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
                MATCH path = (start:Address {{address: $source_address}})-[rels:TO*BFS]->(target:Address {{address: $target_address}})
                WHERE {where_clause}

                // Collect all distinct nodes from the matched paths
                UNWIND nodes(path) AS node
                WITH path, COLLECT(DISTINCT node) AS all_nodes

                // Group relationships by their from and to addresses
                UNWIND relationships(path) AS rel
                WITH all_nodes,
                     startNode(rel).address AS from_id,
                     endNode(rel).address AS to_id,
                     rel
                WITH all_nodes, from_id, to_id, COLLECT(rel) AS rels
                WITH all_nodes, from_id, to_id, head(rels) AS edge_data

                // Build the Address node objects with updated properties
                UNWIND all_nodes AS node
                WITH
                     COLLECT(DISTINCT {{
                         id: node.address,
                         type: 'node',
                         label: 'address',
                         address: node.address,
                         transfer_count: node.transfer_count,
                         neighbor_count: node.neighbor_count,
                         first_activity_block_height: node.first_activity_block_height,
                         first_activity_timestamp: node.first_activity_timestamp,
                         last_activity_block_height: node.last_activity_block_height,
                         last_activity_timestamp: node.last_activity_timestamp,
                         badges: coalesce(node.labels, []),
                         community_id: coalesce(node.community_id, 0),
                         community_page_rank: coalesce(node.community_page_rank, 0.0)
                     }}) AS address_nodes,
                     from_id, to_id, edge_data

                // Build the TO edge objects with updated properties
                WITH address_nodes, COLLECT(DISTINCT {{
                    id: from_id + '-' + to_id + '-' + edge_data.asset + '-' + edge_data.asset_contract,
                    type: 'edge',
                    from_id: from_id,
                    to_id: to_id,
                    volume: edge_data.volume,
                    transfer_count: edge_data.transfer_count,
                    first_activity_block_height: edge_data.first_activity_block_height,
                    first_activity_timestamp: edge_data.first_activity_timestamp,
                    last_activity_block_height: edge_data.last_activity_block_height,
                    last_activity_timestamp: edge_data.last_activity_timestamp,
                    asset: edge_data.asset,
                    asset_contract: edge_data.asset_contract
                }}) AS transfer_edges

                // Unwind the combined list to produce a flat list of elements
                UNWIND (address_nodes + transfer_edges) AS element
                RETURN DISTINCT element
            """

        try:
            with self.graph_database.session() as session:
                result = session.run(query, params)
                json_result = result.data()
                return json_result if json_result else None
        except Exception as e:
            logger.error(f"Error querying graph database: {str(e)}")
            return None

    def get_money_flow_by_path_explore(self,
                                  addresses: List[str],
                                  depth_level: int,
                                  direction: Direction,
                                  assets: List[str] = None):
        """
        Retrieves money flows for addresses with filtering options to reduce noise.
        
        Args:
            addresses: List of wallet addresses to start the exploration from
            depth_level: Number of hops to explore from the starting addresses
            direction: Direction of the relationships to follow
            
        Returns:
            List of nodes and edges representing the money flow graph
        """
        import time
        
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

            // Collect all distinct nodes from the matched paths
            UNWIND nodes(path) AS node
            WITH path, COLLECT(DISTINCT node) AS all_nodes

            // Group relationships by their from and to addresses
            UNWIND relationships(path) AS rel
            WITH all_nodes,
                 startNode(rel).address AS from_id,
                 endNode(rel).address AS to_id,
                 rel
            WITH all_nodes, from_id, to_id, COLLECT(rel) AS rels
            WITH all_nodes, from_id, to_id, head(rels) AS edge_data

            // Build the Address node objects with updated properties
            UNWIND all_nodes AS node
            WITH
                 COLLECT(DISTINCT {{
                     id: node.address,
                     type: 'node',
                     label: 'address',
                     address: node.address,
                     transfer_count: node.transfer_count,
                     neighbor_count: node.neighbor_count,
                     first_activity_block_height: node.first_activity_block_height,
                     first_activity_timestamp: node.first_activity_timestamp,
                     last_activity_block_height: node.last_activity_block_height,
                     last_activity_timestamp: node.last_activity_timestamp,
                     badges: coalesce(node.labels, []),
                     community_id: coalesce(node.community_id, 0),
                     community_page_rank: coalesce(node.community_page_rank, 0.0)
                 }}) AS address_nodes,
                 from_id, to_id, edge_data

            // Build the TO edge objects with updated properties
            WITH address_nodes, COLLECT(DISTINCT {{
                id: from_id + '-' + to_id + '-' + edge_data.asset + '-' + edge_data.asset_contract,
                type: 'edge',
                from_id: from_id,
                to_id: to_id,
                volume: edge_data.volume,
                transfer_count: edge_data.transfer_count,
                first_activity: edge_data.first_activity_block_height,
                first_activity_timestamp: edge_data.first_activity_timestamp,
                last_activity_block_height: edge_data.last_activity_block_height,
                last_activity_timestamp: edge_data.last_activity_timestamp,
                asset: edge_data.asset,
                asset_contract: edge_data.asset_contract
            }}) AS transfer_edges

            // Unwind the combined list to produce a flat list of elements
            UNWIND (address_nodes + transfer_edges) AS element
            RETURN DISTINCT element
        """

        try:
            with self.graph_database.session() as session:
                result = session.run(query, params)
                json_result = result.data()
                
                # Apply post-processing to remove duplicates
                json_result = self._remove_duplicate_elements(json_result)
                
                return json_result if json_result else None
        except Exception as e:
            logger.error(f"Error querying graph database: {str(e)}")
            return None
            
    def _remove_duplicate_elements(self, result):
        """
        Remove duplicate elements from the result based on element ID.
        
        Args:
            result: The result from the Cypher query
            
        Returns:
            The result with duplicates removed
        """
        if not result:
            return result
            
        seen_ids = set()
        unique_results = []
        
        for item in result:
            element = item.get('element', {})
            element_id = element.get('id')
            
            if element_id not in seen_ids:
                seen_ids.add(element_id)
                unique_results.append(item)
        
        return unique_results

    def get_money_flow_schema(self) -> Dict[str, Any]:
        """Get schema information from Memgraph

        Returns:
            Dict containing schema information including node labels, indexes, and vector indexes, example queries
        """

        with self.graph_database.session() as session:
            schema = session.run("CALL llm_util.schema('raw') YIELD schema RETURN schema;").data()
            indexes = session.run("SHOW INDEX INFO").data()
            vector_indexes = session.run("CALL vector_search.show_index_info() YIELD * RETURN *;").data()

            return {
                "schema": schema[0]["schema"],
                "indexes": indexes,
                "vector_indexes": vector_indexes,
                "example_queries": [
                    "MATCH (a:Address {address: 'YOUR_ADDRESS'})-[r:TO]->(b:Address) RETURN a, r, b LIMIT 10",
                    "MATCH path = (start:Address {address: 'SOURCE_ADDRESS'})-[rels:TO*BFS]-(target:Address {address: 'TARGET_ADDRESS'}) RETURN path",
                    "MATCH (a:Address) WHERE a.address IN ['ADDR1', 'ADDR2'] CALL path.expand(a, ['TO'],[],0, 2) YIELD result as path RETURN path"
                ]
            }

    async def money_flow_query(self, query: str) -> Dict[str, Any]:
        """
        Execute money flow query against Memgraph
        
        Args:
            query (str): The Cypher query to execute
            
        Returns:
            Dict containing query results and schema information
        """

        try:
            with self.graph_database.session() as session:
                result = session.run(query)
                json_result = result.data()
                return json_result if json_result else None
        except Exception as e:
            logger.error(f"Error querying graph database: {str(e)}")
            raise e
