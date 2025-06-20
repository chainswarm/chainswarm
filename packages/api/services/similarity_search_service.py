from typing import Any, Dict, Optional, List, Union
from loguru import logger
from neo4j import Driver


class SimilaritySearchService:
    """
    Service for performing vector similarity searches on blockchain addresses.
    
    This service provides methods to find addresses with similar patterns based on
    various embedding types (financial, temporal, network, or joint).
    """
    
    def __init__(self, graph_database: Driver):
        """
        Initialize the SimilaritySearchService.
        
        Args:
            graph_database: Neo4j/Memgraph driver instance
        """
        self.graph_database = graph_database
    
    def _get_embedding_index_name(self, embedding_type: str) -> str:
        """
        Map embedding type to the corresponding vector index name in Memgraph.
        
        Args:
            embedding_type: One of 'financial', 'temporal', 'network', or 'joint'
            
        Returns:
            The name of the vector index in Memgraph
        
        Raises:
            ValueError: If an invalid embedding type is provided
        """
        embedding_mapping = {
            'network': 'NetworkEmbeddings',
        }
        
        if embedding_type.lower() not in embedding_mapping:
            valid_types = ', '.join(embedding_mapping.keys())
            raise ValueError(f"Invalid embedding type: '{embedding_type}'. Valid types are: {valid_types}")
        
        return embedding_mapping[embedding_type.lower()]

    def _get_embedding_dimension(self, embedding_type: str) -> int:
        """
        Get the expected dimension for a given embedding type.
        
        Args:
            embedding_type: One of 'financial', 'temporal', 'network', or 'joint'
            
        Returns:
            The expected dimension of the vector
            
        Raises:
            ValueError: If an invalid embedding type is provided
        """
        dimension_mapping = {
            'network': 6,  # Changed back to 6 to match database schema
        }
        
        if embedding_type.lower() not in dimension_mapping:
            valid_types = ', '.join(dimension_mapping.keys())
            raise ValueError(f"Invalid embedding type: '{embedding_type}'. Valid types are: {valid_types}")
        
        return dimension_mapping[embedding_type.lower()]
    
    def _validate_pattern(self, pattern: dict, expected_keys: List[str]) -> None:
        """
        Validate that a pattern contains all expected keys.
        
        Args:
            pattern: The pattern dictionary to validate
            expected_keys: List of keys that should be present in the pattern
            
        Raises:
            ValueError: If any expected keys are missing from the pattern
        """
        if not pattern:
            raise ValueError("Pattern cannot be None or empty")
            
        missing_keys = [key for key in expected_keys if key not in pattern]
        if missing_keys:
            raise ValueError(f"Missing required keys in pattern: {', '.join(missing_keys)}")

    def _construct_vector_from_network_pattern(self, pattern: dict) -> List[float]:
        """
        Convert a network pattern object to a vector.
        
        Args:
            pattern: Dictionary with network pattern parameters
            
        Returns:
            List of float values representing the vector
        """
        expected_keys = [
            'transfer_count',
            'unique_senders',
            'unique_receivers',
            'neighbor_count',
            'community_id',
            'community_page_rank'
        ]
        self._validate_pattern(pattern, expected_keys)
        
        # Create a 6D vector to match the database schema
        return [
            float(pattern['transfer_count']),
            float(pattern['unique_senders']),
            float(pattern['unique_receivers']),
            float(pattern['neighbor_count']),
            float(pattern['community_id']),
            float(pattern['community_page_rank'])
        ]
        
    def _construct_vector_from_combined_pattern(self, pattern: dict) -> List[float]:
        """
        Convert a combined pattern object to a vector.
        
        Args:
            pattern: Dictionary with combined pattern parameters
            
        Returns:
            List of float values representing the vector
        """

        network_keys = [
            'transfer_count',
            'unique_senders',
            'unique_receivers',
            'neighbor_count',
            'community_id',
            'community_page_rank'
        ]
        
        expected_keys = network_keys
        self._validate_pattern(pattern, expected_keys)

        network_vector = [
            float(pattern['transfer_count']),
            float(pattern['unique_senders']),
            float(pattern['unique_receivers']),
            float(pattern['neighbor_count']),
            float(pattern['community_id']),
            float(pattern['community_page_rank'])
        ]
        
        return network_vector
    
    def _get_vector_from_address(self, address: str, embedding_type: str) -> List[float]:
        """
        Get the embedding vector for an address.
        
        Args:
            address: The blockchain address
            embedding_type: The type of embedding to retrieve
            
        Returns:
            The embedding vector for the address
            
        Raises:
            ValueError: If the address is not found or has no embedding
        """
        with self.graph_database.session() as session:
            result = session.run(
                f"MATCH (a:Address {{address: $address}}) RETURN a.{embedding_type}_embedding AS embedding",
                {"address": address}
            )
            record = result.single()
            
            if not record or not record["embedding"]:
                raise ValueError(f"Address '{address}' not found or has no {embedding_type} embedding")
            
            vector = record["embedding"]
            logger.debug(f"Retrieved {embedding_type} vector for address {address}: {vector}, length: {len(vector)}")
            
            return vector
    
    def _apply_dimension_weights(self, vector: List[float], weights: Optional[dict] = None) -> List[float]:
        """
        Apply dimension weights to a vector.
        
        Args:
            vector: The vector to apply weights to
            weights: Optional dictionary of dimension weights
            
        Returns:
            The weighted vector
        """
        if not weights:
            return vector
            
        # For network embeddings, the dimensions are:
        # [transfer_count, unique_senders, unique_receivers, neighbor_count, community_id, community_page_rank]
        if len(vector) == 6:  # Network embedding
            weight_values = [
                weights.get('transfer_count', 1.0),
                weights.get('unique_senders', 1.0),
                weights.get('unique_receivers', 1.0),
                weights.get('neighbor_count', 1.0),
                weights.get('community_id', 1.0),
                weights.get('community_page_rank', 1.0)
            ]
            
            # Apply weights to vector
            return [v * w for v, w in zip(vector, weight_values)]
            
        # For other embedding types, just return the original vector
        return vector
    
    def _convert_similarity_metric(self, metric: str) -> str:
        """
        Convert API similarity metric name to Memgraph metric name.
        
        Args:
            metric: The API metric name
            
        Returns:
            The corresponding Memgraph metric name
        """
        metric_mapping = {
            'cosine': 'cos'
        }
        
        if metric.lower() not in metric_mapping:
            valid_metrics = ', '.join(metric_mapping.keys())
            raise ValueError(f"Invalid similarity metric: '{metric}'. Valid metrics are: {valid_metrics}")
        
        return metric_mapping[metric.lower()]
            
    def find_similar_addresses(
        self,
        embedding_type: str,
        query_type: str,
        reference_address: Optional[str] = None,
        network_pattern: Optional[dict] = None,
        dimension_weights: Optional[dict] = None,
        limit: int = 10,
        similarity_metric: str = "cosine",
        min_similarity_score: Optional[float] = None
    ):
        """
        Find addresses similar to a query vector or reference address.
        
        Args:
            embedding_type: Type of embedding to use ('financial', 'temporal', 'network', or 'joint')
            query_type: How to specify the search query ('by_address', 'by_financial_pattern', etc.)
            reference_address: Address to use as reference when query_type is 'by_address'
            network_pattern: Network pattern when query_type is 'by_network_pattern'
            limit: Number of similar nodes to retrieve
            similarity_metric: Similarity metric to use
            min_similarity_score: Optional minimum similarity threshold
            
        Returns:
            A list of similar address nodes with similarity scores
        """
        # Get query vector based on query type
        query_vector = None
        
        if query_type == 'by_address':
            if not reference_address:
                raise ValueError("Reference address must be provided when query_type is 'by_address'")
            query_vector = self._get_vector_from_address(reference_address, embedding_type)

        elif query_type == 'by_network_pattern':
            if embedding_type != 'network':
                raise ValueError(f"Network pattern can only be used with 'network' embedding type, not '{embedding_type}'")
            if not network_pattern:
                raise ValueError("Network pattern must be provided when query_type is 'by_network_pattern'")
            query_vector = self._construct_vector_from_network_pattern(network_pattern)
            logger.debug(f"Constructed network vector: {query_vector}, length: {len(query_vector)}")

        else:
            raise ValueError(f"Invalid query type: '{query_type}'")
            
        # Validate the query vector dimension
        expected_dimension = self._get_embedding_dimension(embedding_type)
        logger.debug(f"Expected dimension for {embedding_type} embedding: {expected_dimension}")
        if len(query_vector) != expected_dimension:
            logger.error(f"Dimension mismatch: Query vector dimension ({len(query_vector)}) does not match expected dimension for {embedding_type} embedding ({expected_dimension})")
            raise ValueError(f"Query vector dimension ({len(query_vector)}) does not match expected dimension for {embedding_type} embedding ({expected_dimension})")
            
        # Apply dimension weights if provided
        if dimension_weights:
            original_vector = query_vector.copy()
            query_vector = self._apply_dimension_weights(query_vector, dimension_weights)
            logger.debug(f"Applied dimension weights: {dimension_weights}")
            logger.debug(f"Original vector: {original_vector}")
            logger.debug(f"Weighted vector: {query_vector}")
        
        # Get index name and convert similarity metric
        index_name = self._get_embedding_index_name(embedding_type)
        memgraph_metric = self._convert_similarity_metric(similarity_metric)
        
        # Construct the similarity search query
        min_similarity_clause = f" AND similarity >= {min_similarity_score}" if min_similarity_score is not None else ""
        
        # Simple query using Memgraph vector search
        query = f"""
        CALL vector_search.search("{index_name}", $limit, $query_vector)
        YIELD node, similarity
        WITH node, similarity
        WHERE node:Address{min_similarity_clause}
        
        // Return only the matching address nodes with similarity scores
        RETURN {{
            id: node.address,
            type: 'node',
            label: 'address',
            address: node.address,
            transfer_count: node.transfer_count,
            neighbor_count: node.neighbor_count,
            first_transfer_block_height: node.first_transfer_block_height,
            first_transfer_timestamp: node.first_transfer_timestamp,
            last_transfer_block_height: node.last_transfer_block_height,
            last_transfer_timestamp: node.last_transfer_timestamp,
            badges: coalesce(node.labels, []),
            community_id: coalesce(node.community_id, 0),
            community_page_rank: coalesce(node.community_page_rank, 0.0),
            similarity_score: similarity
        }} AS result
        ORDER BY similarity DESC
        """
        
        try:
            with self.graph_database.session() as session:
                params = {
                    'query_vector': query_vector,
                    'limit': limit
                }
                
                # Add reference_address to params if query_type is by_address
                if query_type == 'by_address':
                    params['reference_address'] = reference_address
                # Run the regular query
                result = session.run(query, params)
                json_result = result.data()
                
                # For by_address query type, add the reference address to the results
                if query_type == 'by_address':
                    # Get the reference address with a simple query
                    ref_query = """
                    MATCH (a:Address {address: $address})
                    RETURN {
                        id: a.address,
                        type: 'node',
                        label: 'address',
                        address: a.address,
                        transfer_count: a.transfer_count,
                        neighbor_count: a.neighbor_count,
                        first_transfer_block_height: a.first_transfer_block_height,
                        first_transfer_timestamp: a.first_transfer_timestamp,
                        last_transfer_block_height: a.last_transfer_block_height,
                        last_transfer_timestamp: a.last_transfer_timestamp,
                        badges: coalesce(a.labels, []),
                        community_id: coalesce(a.community_id, 0),
                        community_page_rank: coalesce(a.community_page_rank, 0.0),
                        similarity_score: 1.0  // Fixed: Use constant 1.0 instead of undefined 'similarity' variable
                    } AS result
                    """
                    ref_result = session.run(ref_query, {"address": reference_address})
                    ref_data = ref_result.data()
                    
                    # Combine results
                    combined_results = []
                    if ref_data:
                        combined_results.extend([item["result"] for item in ref_data])
                    if json_result:
                        combined_results.extend([item["result"] for item in json_result])
                    
                    # Debug log
                    if combined_results:
                        logger.debug(f"Result structure: {combined_results[0].keys() if combined_results else None}")
                    
                    return combined_results if combined_results else None
                else:
                    # For other query types, just return the results
                    if json_result:
                        logger.debug(f"Result structure: {json_result[0].keys() if json_result else None}")
                        transformed_result = [item["result"] for item in json_result]
                        return transformed_result
                    return None
        except Exception as e:
            logger.error(f"Error performing similarity search: {str(e)}")
            return None

    def find_similar_addresses_by_address(
        self,
        address: str,
        embedding_type: str = "network",
        dimension_weights: Optional[dict] = None,
        limit: int = 10,
        similarity_metric: str = "cosine",
        min_similarity_score: Optional[float] = None
    ):
        """
        Find addresses similar to a reference address.
        
        Args:
            address: The reference address to find similar addresses for
            embedding_type: Type of embedding to use
            limit: Number of similar nodes to retrieve
            similarity_metric: Similarity metric to use
            min_similarity_score: Optional minimum similarity threshold
            
        Returns:
            A list of similar address nodes with similarity scores
        """
        return self.find_similar_addresses(
            embedding_type=embedding_type,
            query_type='by_address',
            reference_address=address,
            dimension_weights=dimension_weights,
            limit=limit,
            similarity_metric=similarity_metric,
            min_similarity_score=min_similarity_score
        )
        
    def find_similar_addresses_raw(
        self,
        embedding_type: str,
        query_type: str,
        reference_address: Optional[str] = None,
        network_pattern: Optional[dict] = None,
        dimension_weights: Optional[dict] = None,
        limit: int = 10,
        similarity_metric: str = "cosine",
        min_similarity_score: Optional[float] = None
    ):
        """
        Find addresses similar to a query vector or reference address, returning raw nodes.
        
        This method is similar to find_similar_addresses but returns raw nodes from Memgraph
        instead of transforming them to a specific structure. This is useful for MCP tools
        that need access to the raw node data.
        
        Args:
            embedding_type: Type of embedding to use ('financial', 'temporal', 'network', or 'joint')
            query_type: How to specify the search query ('by_address', 'by_financial_pattern', etc.)
            reference_address: Address to use as reference when query_type is 'by_address'
            network_pattern: Network pattern when query_type is 'by_network_pattern'
            limit: Number of similar nodes to retrieve
            similarity_metric: Similarity metric to use
            min_similarity_score: Optional minimum similarity threshold
            
        Returns:
            A list of dictionaries containing raw node data and similarity scores
        """
        # Get query vector based on query type
        query_vector = None
        
        if query_type == 'by_address':
            if not reference_address:
                raise ValueError("Reference address must be provided when query_type is 'by_address'")
            query_vector = self._get_vector_from_address(reference_address, embedding_type)

        elif query_type == 'by_network_pattern':
            if embedding_type != 'network':
                raise ValueError(f"Network pattern can only be used with 'network' embedding type, not '{embedding_type}'")
            if not network_pattern:
                raise ValueError("Network pattern must be provided when query_type is 'by_network_pattern'")
            query_vector = self._construct_vector_from_network_pattern(network_pattern)
            logger.debug(f"Constructed network vector (raw): {query_vector}, length: {len(query_vector)}")

        else:
            raise ValueError(f"Invalid query type: '{query_type}'")
            
        # Validate the query vector dimension
        expected_dimension = self._get_embedding_dimension(embedding_type)
        logger.debug(f"Expected dimension for {embedding_type} embedding (raw): {expected_dimension}")
        if len(query_vector) != expected_dimension:
            logger.error(f"Dimension mismatch (raw): Query vector dimension ({len(query_vector)}) does not match expected dimension for {embedding_type} embedding ({expected_dimension})")
            raise ValueError(f"Query vector dimension ({len(query_vector)}) does not match expected dimension for {embedding_type} embedding ({expected_dimension})")
            
        # Apply dimension weights if provided
        if dimension_weights:
            original_vector = query_vector.copy()
            query_vector = self._apply_dimension_weights(query_vector, dimension_weights)
            logger.debug(f"Applied dimension weights (raw): {dimension_weights}")
            logger.debug(f"Original vector (raw): {original_vector}")
            logger.debug(f"Weighted vector (raw): {query_vector}")
        
        # Get index name and convert similarity metric
        index_name = self._get_embedding_index_name(embedding_type)
        memgraph_metric = self._convert_similarity_metric(similarity_metric)
        
        # Construct the similarity search query
        min_similarity_clause = f" AND similarity >= {min_similarity_score}" if min_similarity_score is not None else ""
        
        # Query that returns raw nodes with similarity scores
        query = f"""
        CALL vector_search.search("{index_name}", $limit, $query_vector)
        YIELD node, similarity
        WITH node, similarity
        WHERE node:Address{min_similarity_clause}
        
        // Return raw nodes with similarity scores
        RETURN node, similarity as similarity_score
        ORDER BY similarity_score DESC
        """
        
        try:
            with self.graph_database.session() as session:
                params = {
                    'query_vector': query_vector,
                    'limit': limit
                }
                
                # Add reference_address to params if query_type is by_address
                if query_type == 'by_address':
                    params['reference_address'] = reference_address
                
                # Run the query
                result = session.run(query, params)
                records = result.data()
                
                # For by_address query type, add the reference address to the results
                if query_type == 'by_address' and reference_address:
                    # Get the reference address with a simple query
                    ref_query = """
                    MATCH (a:Address {address: $address})
                    RETURN a as node, 1.0 as similarity_score  // This was already correct with a constant value
                    """
                    ref_result = session.run(ref_query, {"address": reference_address})
                    ref_data = ref_result.data()
                    
                    # Combine results
                    combined_results = []
                    if ref_data:
                        combined_results.extend(ref_data)
                    if records:
                        combined_results.extend(records)
                    
                    return combined_results if combined_results else None
                else:
                    # For other query types, just return the results
                    return records if records else None
        except Exception as e:
            logger.error(f"Error performing raw similarity search: {str(e)}")
            return None