from fastapi import APIRouter, Path, Query, HTTPException
from packages.api.services.block_stream_service import BlockStreamService
from packages.indexers.base import get_clickhouse_connection_string


router = APIRouter(
    prefix="/substrate",
    tags=["block-stream"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"}
    }
)

@router.get(
    "/{network}/block-stream/blocks",
    summary="Get Blocks by Range",
    description=(
        "Retrieves blocks within a specified height range. Max 100 blocks.\n\n"
        "This endpoint provides detailed information about blocks, including transactions and events. "
        "The start_height and end_height parameters are required to specify the block range."
    ),
    response_description="List of blocks with transactions and events",
    responses={
        200: {"description": "Blocks retrieved successfully"},
        400: {"description": "Invalid parameters"},
        500: {"description": "Internal server error"}
    }
)
async def get_blocks_by_block_height_range(
    network: str = Path(..., description="The blockchain network identifier", example="torus"),
    start_height: int = Query(..., description="Starting block height (inclusive)", ge=0),
    end_height: int = Query(..., description="Ending block height (inclusive)", ge=0)
):
    try:
        # Validate parameters
        if start_height > end_height:
            raise HTTPException(
                status_code=400,
                detail="start_height must be less than or equal to end_height"
            )
        
        if end_height - start_height > 100:
            raise HTTPException(
                status_code=400,
                detail="Block range too large. Maximum range is 1000 blocks."
            )
        
        block_stream_service = BlockStreamService(get_clickhouse_connection_string(network), network)
        blocks = block_stream_service.get_blocks_by_block_height_range(start_height, end_height)
        
        return {
            "network": network,
            "start_height": start_height,
            "end_height": end_height,
            "block_count": len(blocks),
            "blocks": blocks
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get(
    "/{network}/block-stream/latest-block",
    summary="Get Latest Block Height",
    description=(
        "Retrieves the height of the latest block stored in the database.\n\n"
        "This endpoint is useful for determining the current state of the blockchain indexer."
    ),
    response_description="Latest block height information",
    responses={
        200: {"description": "Latest block height retrieved successfully"},
        500: {"description": "Internal server error"}
    }
)
async def get_latest_block_height(
    network: str = Path(..., description="The blockchain network identifier", example="torus")
):
    try:
        block_stream_service = BlockStreamService(get_clickhouse_connection_string(network), network)
        latest_height = block_stream_service.get_latest_block_height()
        
        return {
            "network": network,
            "latest_block_height": latest_height
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get(
    "/{network}/block-stream/address/{address}/blocks",
    summary="Get Blocks by Address",
    description=(
        "Retrieves blocks that contain transactions involving the specified address.\n\n"
        "This endpoint provides detailed information about blocks where the specified address "
        "was involved in transactions, either as sender or receiver."
    ),
    response_description="List of blocks involving the address",
    responses={
        200: {"description": "Blocks retrieved successfully"},
        500: {"description": "Internal server error"}
    }
)
async def get_blocks_by_address(
    network: str = Path(..., description="The blockchain network identifier", example="torus"),
    address: str = Path(..., description="The blockchain address to query", example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"),
    limit: int = Query(100, description="Maximum number of blocks to return", ge=1, le=100),
    offset: int = Query(0, description="Offset for pagination", ge=0)
):
    try:
        block_stream_service = BlockStreamService(get_clickhouse_connection_string(network), network)
        blocks = block_stream_service.get_blocks_by_address(address, limit, offset)
        
        return {
            "network": network,
            "address": address,
            "block_count": len(blocks),
            "blocks": blocks
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get(
    "/{network}/block-stream/indexing-status",
    summary="Get Indexing Status",
    description=(
        "Retrieves the current status of the block stream indexing process.\n\n"
        "This endpoint provides detailed information about the indexing process, including:\n"
        "- Latest indexed block height\n"
        "- Current blockchain height\n"
        "- Gap between indexed and current height\n"
        "- Active indexing processes\n"
        "- Whether all historical partitions are complete\n"
        "- Whether the indexer is running in continuous mode only\n"
        "- Detailed partition information including:\n"
        "  - Remaining blocks for each partition\n"
        "  - Remaining block ranges that need to be indexed\n"
    ),
    response_description="Indexing status information",
    responses={
        200: {"description": "Indexing status retrieved successfully"},
        500: {"description": "Internal server error"}
    }
)
async def get_indexing_status(
    network: str = Path(..., description="The blockchain network identifier", example="torus")
):
    try:
        block_stream_service = BlockStreamService(get_clickhouse_connection_string(network), network)
        status = block_stream_service.get_indexing_status()
        
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")