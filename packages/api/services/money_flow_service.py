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
                MATCH path = (start:Address {{address: $source_address}})-[rels:TO*BFS]-(target:Address {{address: $target_address}})
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
                         volume_in: node.volume_in,
                         volume_out: node.volume_out,
                         transfer_count: node.transfer_count,
                         neighbor_count: node.neighbor_count,
                         first_transfer_block_height: node.first_transfer_block_height,
                         first_transfer_timestamp: node.first_transfer_timestamp,
                         last_transfer_block_height: node.last_transfer_block_height,
                         last_transfer_timestamp: node.last_transfer_timestamp,
                         badges: coalesce(node.labels, []),
                         community_id: coalesce(node.community_id, 0),
                         community_page_rank: coalesce(node.community_page_rank, 0.0)
                     }}) AS address_nodes,
                     from_id, to_id, edge_data

                // Build the TO edge objects with updated properties
                WITH address_nodes, COLLECT(DISTINCT {{
                    id: from_id + '-' + to_id,
                    type: 'edge',
                    from_id: from_id,
                    to_id: to_id,
                    volume: edge_data.volume,
                    min_amount: edge_data.min_amount,
                    max_amount: edge_data.max_amount,
                    transfer_count: edge_data.transfer_count,
                    tx_frequency: edge_data.tx_frequency,
                    first_transfer_block_height: edge_data.first_transfer_block_height,
                    first_transfer_timestamp: edge_data.first_transfer_timestamp,
                    last_transfer_block_height: edge_data.last_transfer_block_height,
                    last_transfer_timestamp: edge_data.last_transfer_timestamp
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
                     volume_in: node.volume_in,
                     volume_out: node.volume_out,
                     transfer_count: node.transfer_count,
                     neighbor_count: node.neighbor_count,
                     first_transfer_block_height: node.first_transfer_block_height,
                     first_transfer_timestamp: node.first_transfer_timestamp,
                     last_transfer_block_height: node.last_transfer_block_height,
                     last_transfer_timestamp: node.last_transfer_timestamp,
                     badges: coalesce(node.labels, []),
                     community_id: coalesce(node.community_id, 0),
                     community_page_rank: coalesce(node.community_page_rank, 0.0)
                 }}) AS address_nodes,
                 from_id, to_id, edge_data

            // Build the TO edge objects with updated properties
            WITH address_nodes, COLLECT(DISTINCT {{
                id: from_id + '-' + to_id,
                type: 'edge',
                from_id: from_id,
                to_id: to_id,
                volume: edge_data.volume,
                min_amount: edge_data.min_amount,
                max_amount: edge_data.max_amount,
                transfer_count: edge_data.transfer_count,
                tx_frequency: edge_data.tx_frequency,
                first_transfer_block_height: edge_data.first_transfer_block_height,
                first_transfer_timestamp: edge_data.first_transfer_timestamp,
                last_transfer_block_height: edge_data.last_transfer_block_height,
                last_transfer_timestamp: edge_data.last_transfer_timestamp
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


    ###
    ### free query endpoints, VS predefined ones?
    ### OR we build query agent for money flow? and we feed him with schema, and recommendations how to build queries? BUT how to match it with our visualizaiton tool ?
    ### i wanted to store formated data in clickhouse in format ot SESSION | NODES | EDGES and fetch by session id and merge it and then visualize
    ### 2 modes -> visuzalization for tool AND AI agent interaction, but later i want to visualize  agent interaction in tool, so i need some output format, but i can instructc agent to use this format
    ### and similiarity search??? TOOLS or not TOOLS ... i guess agent will be able to build queries by itself, but i need to provide him with some examples and schema, lets be modern
    ### OK... so ... should we use tools as proxy or just memgraph directly, and bypass extra layer which is not needed at all ?????
    ### indexer -> memgraph -> agent, and aget will build vizualizations as we will tell him what output he should use, and we wills tore those wizualizations, client side artifacts !!!!

    ## agent api will return vizualization data: graph, table, timeseries
    ## UI will have chat  + artifacts panel

    """
    
    current plan: 
    - we build agent talking directly with memgraph, clickhouse
    - we use API to interact with UI(graph, table, timeseries)
    
    - or we build MCP support to proxy schema and querying?
    
    those are separated parts, 
    
    
    API -> UI
    MCP -> Claude / External tools integration
    
    
    OR finish for now, shortest path vizualuization in app? then add similiarity search, communities, balance trackings, cycles etc... ?
    
    
    1) offer visualization tools
    2) offer agent to query money flow and balance tracking
    3) integrate visualization result with agent
    4) build individual agents tp the visualization tools
    5) or use agent as QUERY builder !!
    
    """
    
    def get_aggregated_communities(self):
        """Get aggregated communities and subcommunities with their relationships."""
        query = """
        // First, collect all communities and their addresses
        MATCH (a:Address)
        WHERE a.community_id IS NOT NULL
        WITH a.community_id AS community_id, COLLECT(a) AS community_addresses
        
        // Aggregate community-level metrics
        WITH community_id,
             REDUCE(volume_in = 0, addr IN community_addresses |
                    volume_in + CASE WHEN addr.volume_in IS NOT NULL THEN addr.volume_in ELSE 0 END) AS total_volume_in,
             REDUCE(volume_out = 0, addr IN community_addresses |
                    volume_out + CASE WHEN addr.volume_out IS NOT NULL THEN addr.volume_out ELSE 0 END) AS total_volume_out,
             REDUCE(transfer_count = 0, addr IN community_addresses |
                    transfer_count + CASE WHEN addr.transfer_count IS NOT NULL THEN addr.transfer_count ELSE 0 END) AS total_transfers,
             SIZE(community_addresses) AS address_count,
             community_addresses
        
        // Extract subcommunities - extract them directly from addresses
        UNWIND community_addresses AS addr
        WITH community_id, total_volume_in, total_volume_out, total_transfers, address_count,
             addr.community_ids AS subcommunity_ids
        WHERE subcommunity_ids IS NOT NULL
        UNWIND subcommunity_ids AS subcommunity_id
        WITH community_id, total_volume_in, total_volume_out, total_transfers, address_count,
             COLLECT(DISTINCT subcommunity_id) AS all_subcommunity_ids
        
        // Filter out the primary community
        WITH community_id, total_volume_in, total_volume_out, total_transfers, address_count,
             [subcommunity_id IN all_subcommunity_ids WHERE subcommunity_id <> community_id] AS distinct_subcommunities
        
        // Create community nodes
        WITH {
            id: toString(community_id),
            type: 'node',
            label: 'community',
            community_id: community_id,
            volume_in: total_volume_in,
            volume_out: total_volume_out,
            transfer_count: total_transfers,
            address_count: address_count,
            subcommunities: distinct_subcommunities
        } AS community_nodes
        
        // Collect all community nodes
        WITH COLLECT(community_nodes) AS all_community_nodes
        
        // Now find edges between communities
        MATCH (a1:Address)-[r:TO]->(a2:Address)
        WHERE a1.community_id <> a2.community_id
          AND a1.community_id IS NOT NULL
          AND a2.community_id IS NOT NULL
        WITH all_community_nodes,
             a1.community_id AS from_community_id,
             a2.community_id AS to_community_id,
             SUM(r.volume) AS total_volume,
             COUNT(r) AS transfer_count,
             AVG(r.min_amount) AS min_amount,
             AVG(r.max_amount) AS max_amount
        
        // Create community edge objects
        WITH all_community_nodes, {
            id: toString(from_community_id) + '-' + toString(to_community_id),
            type: 'edge',
            from_id: toString(from_community_id),
            to_id: toString(to_community_id),
            volume: total_volume,
            min_amount: min_amount,
            max_amount: max_amount,
            transfer_count: transfer_count
        } AS community_edges
        
        // Combine nodes and edges into a single result
        WITH all_community_nodes, COLLECT(community_edges) AS all_community_edges
        UNWIND (all_community_nodes + all_community_edges) AS element
        RETURN DISTINCT element
        """
        
        try:
            with self.graph_database.session() as session:
                result = session.run(query)
                json_result = result.data()
                return json_result if json_result else []
        except Exception as e:
            logger.error(f"Error querying community aggregation: {str(e)}")
            return []
            
    def get_fan_in_patterns(
        self,
        min_sources: int = 5,          # Minimum number of source addresses
        time_window: int = 86400,      # Time window in seconds (24h)
        min_volume: int = 1000000000,  # Minimum total volume
        min_transfer_count: Optional[int] = None,  # Minimum transfer count per relationship
        max_inactive_days: Optional[int] = None,   # Maximum days of inactivity
        max_results: int = 100         # Maximum results
    ):
        """
        Find addresses receiving funds from multiple source addresses within a specific time window.
        
        Args:
            min_sources: Minimum number of source addresses required
            time_window: Time window in seconds to consider for concentrated transfers
            min_volume: Minimum total volume for the pattern
            min_transfer_count: Optional minimum transfers per relationship
            max_inactive_days: Optional maximum days since last activity
            max_results: Maximum number of results to return
            
        Returns:
            List of fan-in patterns with details about source addresses and volumes
        """
        # Build additional filters
        rel_filters = []
        if min_transfer_count is not None:
            rel_filters.append(f"r.transfer_count >= {min_transfer_count}")
            
        rel_filter = ""
        if rel_filters:
            rel_filter = f"AND {' AND '.join(rel_filters)}"
            
        # Time-based address filtering
        time_filter = ""
        if max_inactive_days is not None and max_inactive_days > 0:
            import time
            current_timestamp = int(time.time()) * 1000  # Current time in milliseconds
            max_inactive_ms = max_inactive_days * 24 * 60 * 60 * 1000  # Convert days to milliseconds
            min_timestamp = current_timestamp - max_inactive_ms
            time_filter = f"AND target.last_transfer_timestamp >= {min_timestamp}"
        
        query = f"""
        // Find addresses that receive funds from multiple sources
        MATCH (source:Address)-[r:TO]->(target:Address)
        WHERE r.volume >= $min_volume {rel_filter} {time_filter}
        WITH target,
             count(DISTINCT source) AS source_count,
             sum(r.volume) AS total_volume
        WHERE source_count >= $min_sources
          AND total_volume >= $min_volume
        
        // Get all incoming transactions for these targets
        MATCH (source:Address)-[r:TO]->(target)
        WHERE r.volume >= $min_volume {rel_filter}
        WITH target, source, r
        ORDER BY r.last_transfer_timestamp DESC
        
        // Group sources by time windows to find fan-in patterns
        WITH target,
             collect({{
                 source: source.address,
                 timestamp: r.last_transfer_timestamp,
                 volume: r.volume,
                 transfer_count: r.transfer_count
             }}) AS incoming_txs
        
        // Analyze time windows for concentrated incoming transfers
        UNWIND incoming_txs AS tx
        WITH target, tx
        ORDER BY tx.timestamp DESC
        WITH target,
             collect(tx) AS recent_txs,
             min(tx.timestamp) AS window_start,
             max(tx.timestamp) AS window_end
        WHERE (window_end - window_start) <= $time_window
          AND size(recent_txs) >= $min_sources
        
        RETURN {{
            target_address: target.address,
            source_count: size(recent_txs),
            total_volume: reduce(total = 0, tx IN recent_txs | total + tx.volume),
            time_window_seconds: (window_end - window_start)/1000,  // Convert ms to seconds
            first_transfer: window_start,
            last_transfer: window_end,
            sources: [tx IN recent_txs | {{
                address: tx.source,
                volume: tx.volume,
                timestamp: tx.timestamp,
                transfer_count: tx.transfer_count
            }}]
        }} AS fan_in_pattern
        ORDER BY fan_in_pattern.source_count DESC, fan_in_pattern.total_volume DESC
        LIMIT $max_results
        """
        
        try:
            with self.graph_database.session() as session:
                params = {
                    'min_sources': min_sources,
                    'time_window': time_window * 1000,  # Convert to milliseconds
                    'min_volume': min_volume,
                    'max_results': max_results
                }
                result = session.run(query, params)
                return result.data()
        except Exception as e:
            logger.error(f"Error querying fan-in patterns: {str(e)}")
            return []
            
    def get_fan_out_patterns(
        self,
        min_targets: int = 5,          # Minimum number of target addresses
        time_window: int = 86400,      # Time window in seconds (24h)
        min_volume: int = 1000000000,  # Minimum total volume
        min_transfer_count: Optional[int] = None,  # Minimum transfer count per relationship
        max_inactive_days: Optional[int] = None,   # Maximum days of inactivity
        max_results: int = 100         # Maximum results
    ):
        """
        Find addresses sending funds to multiple target addresses within a specific time window.
        
        Args:
            min_targets: Minimum number of target addresses required
            time_window: Time window in seconds to consider for concentrated transfers
            min_volume: Minimum total volume for the pattern
            min_transfer_count: Optional minimum transfers per relationship
            max_inactive_days: Optional maximum days since last activity
            max_results: Maximum number of results to return
            
        Returns:
            List of fan-out patterns with details about target addresses and volumes
        """
        # Build additional filters
        rel_filters = []
        if min_transfer_count is not None:
            rel_filters.append(f"r.transfer_count >= {min_transfer_count}")
            
        rel_filter = ""
        if rel_filters:
            rel_filter = f"AND {' AND '.join(rel_filters)}"
            
        # Time-based address filtering
        time_filter = ""
        if max_inactive_days is not None and max_inactive_days > 0:
            import time
            current_timestamp = int(time.time()) * 1000  # Current time in milliseconds
            max_inactive_ms = max_inactive_days * 24 * 60 * 60 * 1000  # Convert days to milliseconds
            min_timestamp = current_timestamp - max_inactive_ms
            time_filter = f"AND source.last_transfer_timestamp >= {min_timestamp}"
        
        query = f"""
        // Find addresses that send funds to multiple targets
        MATCH (source:Address)-[r:TO]->(target:Address)
        WHERE r.volume >= $min_volume {rel_filter} {time_filter}
        WITH source,
             count(DISTINCT target) AS target_count,
             sum(r.volume) AS total_volume
        WHERE target_count >= $min_targets
          AND total_volume >= $min_volume
        
        // Get all outgoing transactions for these sources
        MATCH (source)-[r:TO]->(target:Address)
        WHERE r.volume >= $min_volume {rel_filter}
        WITH source, target, r
        ORDER BY r.last_transfer_timestamp DESC
        
        // Group targets by time windows to find fan-out patterns
        WITH source,
             collect({{
                 target: target.address,
                 timestamp: r.last_transfer_timestamp,
                 volume: r.volume,
                 transfer_count: r.transfer_count
             }}) AS outgoing_txs
        
        // Analyze time windows for concentrated outgoing transfers
        UNWIND outgoing_txs AS tx
        WITH source, tx
        ORDER BY tx.timestamp DESC
        WITH source,
             collect(tx) AS recent_txs,
             min(tx.timestamp) AS window_start,
             max(tx.timestamp) AS window_end
        WHERE (window_end - window_start) <= $time_window
          AND size(recent_txs) >= $min_targets
        
        RETURN {{
            source_address: source.address,
            target_count: size(recent_txs),
            total_volume: reduce(total = 0, tx IN recent_txs | total + tx.volume),
            time_window_seconds: (window_end - window_start)/1000,  // Convert ms to seconds
            first_transfer: window_start,
            last_transfer: window_end,
            targets: [tx IN recent_txs | {{
                address: tx.target,
                volume: tx.volume,
                timestamp: tx.timestamp,
                transfer_count: tx.transfer_count
            }}]
        }} AS fan_out_pattern
        ORDER BY fan_out_pattern.target_count DESC, fan_out_pattern.total_volume DESC
        LIMIT $max_results
        """
        
        try:
            with self.graph_database.session() as session:
                params = {
                    'min_targets': min_targets,
                    'time_window': time_window * 1000,  # Convert to milliseconds
                    'min_volume': min_volume,
                    'max_results': max_results
                }
                result = session.run(query, params)
                return result.data()
        except Exception as e:
            logger.error(f"Error querying fan-out patterns: {str(e)}")
            return []
            
    def analyze_transaction_frequency(
        self,
        address: Optional[str] = None,
        min_transfers: int = 5,
        pattern_type: str = 'all'  # 'all', 'regular', 'burst', 'inconsistent'
    ):
        """
        Analyze transaction frequency patterns between addresses.
        
        Args:
            address: Optional specific address to analyze
            min_transfers: Minimum number of transfers required for meaningful analysis
            pattern_type: Type of pattern to search for ('regular', 'burst', 'inconsistent', or 'all')
            
        Returns:
            List of transaction frequency patterns with detailed statistics
        """
        address_filter = ""
        if address:
            address_filter = "WHERE startNode(r).address = $address OR endNode(r).address = $address"
        
        query = f"""
        // Find TO relationships with sufficient transfers to analyze
        MATCH ()-[r:TO]->()
        {address_filter}
        WHERE r.transfer_count >= $min_transfers
          AND size(r.tx_frequency) >= $min_transfers - 1
        
        // Calculate statistics about transaction frequency
        WITH r,
             startNode(r) AS source,
             endNode(r) AS target,
             avg(toFloat(r.tx_frequency)) AS avg_interval,
             stdDev(r.tx_frequency) AS std_interval,
             min(r.tx_frequency) AS min_interval,
             max(r.tx_frequency) AS max_interval,
             size(r.tx_frequency) AS interval_count
        
        // Determine pattern type
        WITH r, source, target, avg_interval, std_interval, min_interval, max_interval,
             CASE
               // Regular pattern - low standard deviation relative to average
               WHEN std_interval <= (avg_interval * 0.3) THEN 'regular'
               // Burst pattern - min interval is much smaller than average
               WHEN min_interval <= (avg_interval * 0.2) THEN 'burst'
               // Otherwise inconsistent
               ELSE 'inconsistent'
             END AS pattern
        """
        
        if pattern_type != 'all':
            query += f"\nWHERE pattern = '{pattern_type}'"
        
        query += """
        RETURN {
            source_address: source.address,
            target_address: target.address,
            pattern_type: pattern,
            transfer_count: r.transfer_count,
            avg_block_interval: avg_interval,
            std_block_interval: std_interval,
            min_block_interval: min_interval,
            max_block_interval: max_interval,
            frequency_data: r.tx_frequency,
            first_transfer_timestamp: r.first_transfer_timestamp,
            last_transfer_timestamp: r.last_transfer_timestamp
        } AS frequency_pattern
        ORDER BY
            CASE pattern
                WHEN 'regular' THEN 1
                WHEN 'burst' THEN 2
                ELSE 3
            END,
            r.transfer_count DESC
        LIMIT 100
        """
        
        try:
            with self.graph_database.session() as session:
                params = {
                    'address': address,
                    'min_transfers': min_transfers
                }
                result = session.run(query, params)
                return result.data()
        except Exception as e:
            logger.error(f"Error analyzing transaction frequency: {str(e)}")
            return []
            
    def detect_temporal_sequences(
        self,
        min_sequence_length: int = 3,
        max_time_gap: int = 3600,  # in seconds
        min_volume: int = 1000000000
    ):
        """
        Detect sequences of transfers that follow a temporal pattern.
        
        Args:
            min_sequence_length: Minimum number of transfers in sequence
            max_time_gap: Maximum time gap between transfers (in seconds)
            min_volume: Minimum transfer volume to consider
            
        Returns:
            List of temporal sequences with details about each transfer
        """
        query = """
        MATCH path = (a:Address)-[r1:TO]->(b:Address)-[r2:TO]->(c:Address)
        WHERE r1.volume >= $min_volume AND r2.volume >= $min_volume
          AND r2.first_transfer_timestamp >= r1.last_transfer_timestamp
          AND (r2.first_transfer_timestamp - r1.last_transfer_timestamp) <= $max_time_gap
        
        WITH path,
             nodes(path) as addresses,
             relationships(path) as transfers,
             [r1, r2] as r_seq
             
        // For longer sequences, add more path matching
        OPTIONAL MATCH extension = (c:Address)-[r3:TO]->(d:Address)
        WHERE c = addresses[2]
          AND r3.volume >= $min_volume
          AND r3.first_transfer_timestamp >= r_seq[1].last_transfer_timestamp
          AND (r3.first_transfer_timestamp - r_seq[1].last_transfer_timestamp) <= $max_time_gap
          
        WITH CASE WHEN extension IS NULL
                  THEN path
                  ELSE path + extension
             END as extended_path,
             CASE WHEN extension IS NULL
                  THEN r_seq
                  ELSE r_seq + [relationships(extension)[0]]
             END as r_extended_seq
        
        WHERE size(r_extended_seq) >= $min_sequence_length
        
        RETURN {
            sequence_length: size(r_extended_seq),
            total_volume: reduce(total = 0, r IN r_extended_seq | total + r.volume),
            start_timestamp: r_extended_seq[0].first_transfer_timestamp,
            end_timestamp: last(r_extended_seq).last_transfer_timestamp,
            addresses: [a in nodes(extended_path) | a.address],
            transfers: [r in r_extended_seq | {
                from: startNode(r).address,
                to: endNode(r).address,
                volume: r.volume,
                timestamp: r.first_transfer_timestamp
            }]
        } AS temporal_sequence
        ORDER BY temporal_sequence.sequence_length DESC, temporal_sequence.total_volume DESC
        LIMIT 100
        """
        
        try:
            with self.graph_database.session() as session:
                params = {
                    'min_sequence_length': min_sequence_length,
                    'max_time_gap': max_time_gap * 1000,  # Convert to milliseconds
                    'min_volume': min_volume
                }
                result = session.run(query, params)
                return result.data()
        except Exception as e:
            logger.error(f"Error detecting temporal sequences: {str(e)}")
            return []
