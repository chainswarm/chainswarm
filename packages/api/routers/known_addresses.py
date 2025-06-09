from fastapi import APIRouter, Depends, HTTPException, Query, Path
from typing import Optional, List, Dict, Any

from packages.indexers.base import get_clickhouse_connection_string
from packages.api.services.known_addresses_service import KnownAddressesService

router = APIRouter(
    prefix="/substrate",
    tags=["known-addresses"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"}
    }
)

@router.get(
    "/{network}/known-addresses",
    summary="Get known addresses",
    description="Get a list of known addresses for a blockchain network with pagination. Optionally filter by specific addresses."
)
async def get_known_addresses(
    network: str = Path(..., description="The blockchain network identifier", example="torus"),
    addresses: Optional[List[str]] = Query(None, description="Optional list of addresses to filter by"),
    page: int = Query(1, description="Page number (1-based)", ge=1),
    page_size: int = Query(100, description="Number of items per page", ge=1, le=1000)
):
    """
    Get known addresses for a blockchain network with pagination
    """
    try:
        # Get ClickHouse connection parameters
        clickhouse_params = get_clickhouse_connection_string(network)
        
        # Initialize the known addresses service
        known_addresses_service = KnownAddressesService(clickhouse_params)
        
        # Get the known addresses
        result = await known_addresses_service.get_known_addresses(
            network=network,
            addresses=addresses,
            page=page,
            page_size=page_size
        )
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving known addresses: {str(e)}")

# Import endpoint removed - now handled by separate import service