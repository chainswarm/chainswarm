from typing import Optional, List, Dict, Union
from fastapi import APIRouter, Query, Path, HTTPException, Body
from neo4j import GraphDatabase
from enum import Enum
from pydantic import BaseModel, Field

from packages.api.routers import get_memgraph_driver
from packages.api.services.similarity_search_service import SimilaritySearchService

router = APIRouter(
    prefix="/substrate",
    tags=["similarity-search"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"}
    }
)

# Enums for similarity search parameters
class EmbeddingType(str, Enum):
    network = "network"

class QueryType(str, Enum):
    by_address = "by_address"
    by_network_pattern = "by_network_pattern"


class SimilarityMetric(str, Enum):
    cosine = "cosine"

class NetworkPattern(BaseModel):
    community_page_rank : float = Field(..., description="CommunityPageRank score")
    community_id: float = Field(..., description="Community membership ID")
    unique_senders: float = Field(..., description="Number of unique senders")
    unique_receivers: float = Field(..., description="Number of unique receivers")


@router.post(
    "/{network}/similarity-search",
    summary="Find addresses with similar patterns using vector similarity search",
    description=(
        "Retrieves addresses with similar patterns based on vector embeddings.\n\n"
        "**Search Behavior:**\n"
        "The system will perform a vector similarity search using the specified embedding type "
        "to find the most similar addresses based on their embedding vectors."
    ),
    response_description="List of similar addresses with similarity scores",
    responses={
        200: {"description": "Similar addresses retrieved successfully"},
        400: {"description": "Invalid input parameters"},
        404: {"description": "No similar addresses found"},
        500: {"description": "Internal server error"}
    }
)
async def find_similar_addresses(
    network: str = Path(..., description="The blockchain network identifier", example="torus"),
    embedding_type: EmbeddingType = Query(
        EmbeddingType.network,
        description="Type of embedding to use for similarity search"
    ),
    query_type: QueryType = Query(
        ...,
        description="How to specify the search query"
    ),
    reference_address: Optional[str] = Query(
        None,
        description="Reference address to use as the query vector when query_type is 'by_address'",
        example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"
    ),
    network_pattern: Optional[NetworkPattern] = Body(
        None,
        description="Network pattern when query_type is 'by_network_pattern'"
    ),
    limit: int = Query(
        10,
        description="Number of similar addresses to retrieve",
        gt=0,
        le=100
    ),
    similarity_metric: SimilarityMetric = Query(
        SimilarityMetric.cosine,
        description="Similarity metric to use"
    ),
    min_similarity_score: Optional[float] = Query(
        None,
        description="Minimum similarity threshold (0-1)",
        ge=0,
        le=1
    )
):
    # Validate parameter combinations based on query_type
    if query_type == QueryType.by_address and not reference_address:
        raise HTTPException(
            status_code=400,
            detail="Reference address must be provided when query_type is 'by_address'"
        )
    
    if query_type == QueryType.by_network_pattern and not network_pattern:
        raise HTTPException(
            status_code=400,
            detail="Network pattern must be provided when query_type is 'by_network_pattern'"
        )

    memgraph_driver = get_memgraph_driver(network)
    try:
        similarity_search_service = SimilaritySearchService(memgraph_driver)
        result = similarity_search_service.find_similar_addresses(
            embedding_type=embedding_type.value,
            query_type=query_type.value,
            reference_address=reference_address,
            network_pattern=network_pattern.dict() if network_pattern else None,
            limit=limit,
            similarity_metric=similarity_metric.value,
            min_similarity_score=min_similarity_score
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    finally:
        memgraph_driver.close()
    
    if not result:
        return []
    return result

@router.get(
    "/{network}/similarity-search/address/{address}",
    summary="Find addresses similar to a reference address",
    description=(
        "Retrieves addresses with similar patterns to the reference address.\n\n"
        "Uses the specified embedding type to find similar addresses based on "
        "vector similarity to the reference address."
    ),
    response_description="List of similar addresses with similarity scores",
    responses={
        200: {"description": "Similar addresses retrieved successfully"},
        400: {"description": "Invalid input parameters"},
        404: {"description": "No similar addresses found"},
        500: {"description": "Internal server error"}
    }
)
async def find_similar_addresses_by_address(
    network: str = Path(..., description="The blockchain network identifier", example="torus"),
    address: str = Path(..., description="The reference address to find similar addresses to", example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"),
    embedding_type: EmbeddingType = Query(
        EmbeddingType.network,
        description="Type of embedding to use for similarity"
    ),
    limit: int = Query(
        10,
        description="Number of similar addresses to retrieve",
        gt=0,
        le=100
    ),
    similarity_metric: SimilarityMetric = Query(
        SimilarityMetric.cosine,
        description="Similarity metric to use"
    ),
    min_similarity_score: Optional[float] = Query(
        None,
        description="Minimum similarity threshold (0-1)",
        ge=0,
        le=1
    )
):
    memgraph_driver = get_memgraph_driver(network)
    try:
        similarity_search_service = SimilaritySearchService(memgraph_driver)
        result = similarity_search_service.find_similar_addresses_by_address(
            address=address,
            embedding_type=embedding_type.value,
            limit=limit,
            similarity_metric=similarity_metric.value,
            min_similarity_score=min_similarity_score
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    finally:
        memgraph_driver.close()
    
    if not result:
        return []
    return result