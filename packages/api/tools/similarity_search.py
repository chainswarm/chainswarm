from typing import Dict, Any, List, Optional
from neo4j import Driver
from loguru import logger

from packages.api.services.similarity_search_service import SimilaritySearchService


class SimilaritySearchTool:
    def __init__(self, graph_database: Driver):
        self.graph_database = graph_database
        self.similarity_service = SimilaritySearchService(graph_database)

    def get_similarity_search_schema(self) -> dict:
        """
        Get schema information for similarity search
        
        Returns:
            Dict containing schema information including embedding types, query types,
            similarity metrics, pattern structures, and example queries
        """
        return {
            "name": "Similarity Search Schema",
            "description": "Schema for vector similarity search on blockchain addresses",
            "embedding_types": {
                "financial": {
                    "description": "Financial behavior embeddings (6 dimensions)",
                    "dimensions": 6,
                    "components": [
                        "volume_in", "volume_out", "volume_differential",
                        "log_transfer_count", "outgoing_tx_avg_ratio", "incoming_tx_avg_ratio"
                    ]
                },
                "temporal": {
                    "description": "Temporal activity embeddings (4 dimensions)",
                    "dimensions": 4,
                    "components": [
                        "last_transfer_timestamp", "first_transfer_timestamp",
                        "avg_outgoing_tx_frequency", "avg_incoming_tx_frequency"
                    ]
                },
                "network": {
                    "description": "Network structure embeddings (4 dimensions)",
                    "dimensions": 4,
                    "components": [
                        "page_rank", "community_id", "unique_senders", "unique_receivers"
                    ]
                },
                "joint": {
                    "description": "Combined embeddings (14 dimensions)",
                    "dimensions": 14,
                    "components": [
                        "volume_in", "volume_out", "volume_differential",
                        "log_transfer_count", "outgoing_tx_avg_ratio", "incoming_tx_avg_ratio",
                        "last_transfer_timestamp", "first_transfer_timestamp",
                        "avg_outgoing_tx_frequency", "avg_incoming_tx_frequency",
                        "page_rank", "community_id", "unique_senders", "unique_receivers"
                    ]
                }
            },
            "query_types": {
                "by_address": {
                    "description": "Find addresses similar to a reference address",
                    "required_parameters": ["reference_address", "embedding_type"],
                    "compatible_embedding_types": ["financial", "temporal", "network", "joint"]
                },
                "by_financial_pattern": {
                    "description": "Find addresses similar to a financial pattern",
                    "required_parameters": ["financial_pattern"],
                    "compatible_embedding_types": ["financial"]
                },
                "by_temporal_pattern": {
                    "description": "Find addresses similar to a temporal pattern",
                    "required_parameters": ["temporal_pattern"],
                    "compatible_embedding_types": ["temporal"]
                },
                "by_network_pattern": {
                    "description": "Find addresses similar to a network pattern",
                    "required_parameters": ["network_pattern"],
                    "compatible_embedding_types": ["network"]
                },
                "by_combined_pattern": {
                    "description": "Find addresses similar to a combined pattern",
                    "required_parameters": ["combined_pattern"],
                    "compatible_embedding_types": ["joint"]
                }
            },
            "similarity_metrics": {
                "cosine": "Cosine similarity (angle between vectors)",
                "euclidean": "Euclidean distance (L2 norm)",
                "dot_product": "Dot product (inner product)",
                "correlation": "Pearson correlation coefficient"
            },
            "pattern_structures": {
                "financial_pattern": {
                    "volume_in": "float - Incoming volume",
                    "volume_out": "float - Outgoing volume",
                    "volume_differential": "float - Volume differential",
                    "log_transfer_count": "float - Log-scaled transaction count",
                    "outgoing_tx_avg_ratio": "float - Outgoing transaction to average volume ratio",
                    "incoming_tx_avg_ratio": "float - Incoming transaction to average volume ratio"
                },
                "temporal_pattern": {
                    "last_transfer_timestamp": "float - Last activity timestamp",
                    "first_transfer_timestamp": "float - First activity timestamp",
                    "avg_outgoing_tx_frequency": "float - Average outgoing transaction frequency",
                    "avg_incoming_tx_frequency": "float - Average incoming transaction frequency"
                },
                "network_pattern": {
                    "page_rank": "float - PageRank score",
                    "community_id": "float - Community membership ID",
                    "unique_senders": "float - Number of unique senders",
                    "unique_receivers": "float - Number of unique receivers"
                },
                "combined_pattern": {
                    "volume_in": "float - Incoming volume",
                    "volume_out": "float - Outgoing volume",
                    "volume_differential": "float - Volume differential",
                    "log_transfer_count": "float - Log-scaled transaction count",
                    "outgoing_tx_avg_ratio": "float - Outgoing transaction to average volume ratio",
                    "incoming_tx_avg_ratio": "float - Incoming transaction to average volume ratio",
                    "last_transfer_timestamp": "float - Last activity timestamp",
                    "first_transfer_timestamp": "float - First activity timestamp",
                    "avg_outgoing_tx_frequency": "float - Average outgoing transaction frequency",
                    "avg_incoming_tx_frequency": "float - Average incoming transaction frequency",
                    "page_rank": "float - PageRank score",
                    "community_id": "float - Community membership ID",
                    "unique_senders": "float - Number of unique senders",
                    "unique_receivers": "float - Number of unique receivers"
                }
            },
            "example_queries": [
                {
                    "description": "Find addresses similar to a reference address using joint embeddings",
                    "query": {
                        "query_type": "by_address",
                        "reference_address": "5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f",
                        "embedding_type": "joint",
                        "limit": 10,
                        "similarity_metric": "cosine"
                    }
                },
                {
                    "description": "Find addresses with similar financial patterns",
                    "query": {
                        "query_type": "by_financial_pattern",
                        "embedding_type": "financial",
                        "financial_pattern": {
                            "volume_in": 1000000000,
                            "volume_out": 900000000,
                            "volume_differential": 100000000,
                            "log_transfer_count": 4.5,
                            "outgoing_tx_avg_ratio": 0.8,
                            "incoming_tx_avg_ratio": 0.7
                        },
                        "limit": 10,
                        "similarity_metric": "cosine",
                        "min_similarity_score": 0.7
                    }
                }
            ]
        }

    def similarity_search_query(self, query: dict) -> dict:
        """
        Execute similarity search query against Memgraph
        
        Args:
            query (dict): Query parameters including:
                - query_type: How to specify the search query
                - embedding_type: Type of embedding to use
                - reference_address: (Optional) Address to use as reference
                - financial_pattern: (Optional) Financial pattern parameters
                - temporal_pattern: (Optional) Temporal pattern parameters
                - network_pattern: (Optional) Network pattern parameters
                - combined_pattern: (Optional) Combined pattern parameters
                - limit: Number of similar nodes to retrieve
                - similarity_metric: Similarity metric to use
                - min_similarity_score: (Optional) Minimum similarity threshold
                
        Returns:
            Dict containing query results with raw nodes and similarity scores
        """
        try:
            # Extract parameters from the query
            query_type = query.get("query_type")
            embedding_type = query.get("embedding_type", "joint")
            reference_address = query.get("reference_address")
            financial_pattern = query.get("financial_pattern")
            temporal_pattern = query.get("temporal_pattern")
            network_pattern = query.get("network_pattern")
            combined_pattern = query.get("combined_pattern")
            limit = query.get("limit", 10)
            similarity_metric = query.get("similarity_metric", "cosine")
            min_similarity_score = query.get("min_similarity_score")
            
            # Validate required parameters
            if not query_type:
                raise ValueError("query_type is required")
                
            # Validate query_type
            valid_query_types = ["by_address", "by_financial_pattern", "by_temporal_pattern",
                                "by_network_pattern", "by_combined_pattern"]
            if query_type not in valid_query_types:
                raise ValueError(f"Invalid query_type: {query_type}. Valid types are: {', '.join(valid_query_types)}")
                
            # Validate embedding_type
            valid_embedding_types = ["financial", "temporal", "network", "joint"]
            if embedding_type not in valid_embedding_types:
                raise ValueError(f"Invalid embedding_type: {embedding_type}. Valid types are: {', '.join(valid_embedding_types)}")
                
            # Validate parameter combinations based on query_type
            if query_type == "by_address" and not reference_address:
                raise ValueError("reference_address is required when query_type is 'by_address'")
                
            if query_type == "by_financial_pattern" and not financial_pattern:
                raise ValueError("financial_pattern is required when query_type is 'by_financial_pattern'")
                
            if query_type == "by_temporal_pattern" and not temporal_pattern:
                raise ValueError("temporal_pattern is required when query_type is 'by_temporal_pattern'")
                
            if query_type == "by_network_pattern" and not network_pattern:
                raise ValueError("network_pattern is required when query_type is 'by_network_pattern'")
                
            if query_type == "by_combined_pattern" and not combined_pattern:
                raise ValueError("combined_pattern is required when query_type is 'by_combined_pattern'")
                
            # Validate embedding type compatibility with query type
            if query_type == "by_financial_pattern" and embedding_type != "financial":
                raise ValueError("When query_type is 'by_financial_pattern', embedding_type must be 'financial'")
                
            if query_type == "by_temporal_pattern" and embedding_type != "temporal":
                raise ValueError("When query_type is 'by_temporal_pattern', embedding_type must be 'temporal'")
                
            if query_type == "by_network_pattern" and embedding_type != "network":
                raise ValueError("When query_type is 'by_network_pattern', embedding_type must be 'network'")
                
            if query_type == "by_combined_pattern" and embedding_type != "joint":
                raise ValueError("When query_type is 'by_combined_pattern', embedding_type must be 'joint'")
            
            # Execute the similarity search using the raw method
            results = self.similarity_service.find_similar_addresses_raw(
                embedding_type=embedding_type,
                query_type=query_type,
                reference_address=reference_address,
                financial_pattern=financial_pattern,
                temporal_pattern=temporal_pattern,
                network_pattern=network_pattern,
                combined_pattern=combined_pattern,
                limit=limit,
                similarity_metric=similarity_metric,
                min_similarity_score=min_similarity_score
            )
            
            # Process results to return in the expected format
            if not results:
                return {"results": [], "count": 0}
                
            # Transform results to include node properties and similarity score
            processed_results = []
            for result in results:
                node_data = dict(result["node"])
                node_data["similarity_score"] = result["similarity_score"]
                processed_results.append(node_data)
                
            return {
                "results": processed_results,
                "count": len(processed_results)
            }
            
        except ValueError as ve:
            logger.error(f"Validation error in similarity search query: {str(ve)}")
            return {"error": str(ve), "results": [], "count": 0}
        except Exception as e:
            logger.error(f"Error executing similarity search query: {str(e)}")
            return {"error": f"Internal error: {str(e)}", "results": [], "count": 0}

    def close(self):
        if self.graph_database:
            self.graph_database.close()