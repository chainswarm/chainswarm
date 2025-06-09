from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Query, Path, HTTPException, Body

from packages.api.services.balance_tracking_service import BalanceTrackingService
from packages.indexers.base import get_clickhouse_connection_string
from packages.indexers.substrate import get_network_asset

router = APIRouter(
    prefix="/substrate",
    tags=["balance-tracking"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"}
    }
)

@router.get(
    "/{network}/balance-tracking/address/{address}/balance-history",
    summary="Get Address Balance History",
    description=(
        "Retrieves balance history for a specific address on a blockchain network.\n\n"
        "This endpoint provides a paginated list of balance changes for the specified address. "
        "Each record includes the block height, timestamp, and balance components (free, reserved, staked, total)."
    ),
    response_description="Address balance history",
    responses={
        200: {"description": "Balance history retrieved successfully"},
        404: {"description": "Address not found"},
        500: {"description": "Internal server error"}
    }
)
async def get_address_balance_history(
    network: str = Path(..., description="The blockchain network identifier", example="torus"),
    address: str = Path(..., description="The blockchain address to query", example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"),
    page: int = Query(1, description="Page number for pagination", ge=1),
    page_size: int = Query(20, description="Number of records per page", ge=1, le=100),
    assets: List[str] = Query(
        None,
        description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
        example=["TOR"]
    )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]
    
    try:
        balance_tracking_service = BalanceTrackingService(get_clickhouse_connection_string(network))
        result = balance_tracking_service.get_address_balance_history(address, page, page_size, assets)
        
        if not result["items"]:
            raise HTTPException(status_code=404, detail=f"No balance history found for address {address}")
        
        return result
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get(
    "/{network}/balance-tracking/address/{address}/balance-deltas",
    summary="Get Address Balance Deltas",
    description=(
        "Retrieves balance delta changes for a specific address on a blockchain network.\n\n"
        "This endpoint provides a paginated list of balance delta changes for the specified address. "
        "Each record includes the block height, timestamp, and balance delta components "
        "(free_balance_delta, reserved_balance_delta, staked_balance_delta, total_balance_delta)."
    ),
    response_description="Address balance deltas",
    responses={
        200: {"description": "Balance deltas retrieved successfully"},
        404: {"description": "Address not found"},
        500: {"description": "Internal server error"}
    }
)
async def get_address_balance_deltas(
    network: str = Path(..., description="The blockchain network identifier", example="torus"),
    address: str = Path(..., description="The blockchain address to query", example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"),
    page: int = Query(1, description="Page number for pagination", ge=1),
    page_size: int = Query(20, description="Number of records per page", ge=1, le=100),
    assets: List[str] = Query(
        None,
        description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
        example=["TOR"]
    )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]
    
    try:
        balance_tracking_service = BalanceTrackingService(get_clickhouse_connection_string(network))
        result = balance_tracking_service.get_address_balance_deltas(address, page, page_size, assets)
        
        if not result["items"]:
            raise HTTPException(status_code=404, detail=f"No balance deltas found for address {address}")
        
        return result
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get(
    "/{network}/balance-tracking/address/{address}/balance-at-block/{block_height}",
    summary="Get Address Balance at Block",
    description=(
        "Retrieves balance for a specific address at a specific block height.\n\n"
        "This endpoint provides the balance information for the specified address at the given block height. "
        "If the address does not have a balance record at the exact block height, it returns the most recent "
        "balance record before the specified block height."
    ),
    response_description="Address balance at block",
    responses={
        200: {"description": "Balance retrieved successfully"},
        404: {"description": "Address not found or no balance record before the specified block height"},
        500: {"description": "Internal server error"}
    }
)
async def get_address_balance_at_block(
    network: str = Path(..., description="The blockchain network identifier", example="torus"),
    address: str = Path(..., description="The blockchain address to query", example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"),
    block_height: int = Path(..., description="The block height to query at", example=1000),
    assets: List[str] = Query(
        None,
        description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
        example=["TOR"]
    )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]
    
    try:
        balance_tracking_service = BalanceTrackingService(get_clickhouse_connection_string(network))
        result = balance_tracking_service.get_address_balance_at_block(address, block_height, assets)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No balance record found for address {address} at or before block {block_height}"
            )
        
        return result
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get(
    "/{network}/balance-tracking/address/{address}/transactions",
    summary="Get Address Transactions",
    description=(
        "Retrieves transaction history for a specific address on a blockchain network.\n\n"
        "This endpoint provides a paginated list of transactions involving the specified address, "
        "either as sender or receiver. Optionally, you can filter transactions between the specified "
        "address and a target address."
    ),
    response_description="Address transaction history",
    responses={
        200: {"description": "Transaction history retrieved successfully"},
        404: {"description": "Address not found"},
        500: {"description": "Internal server error"}
    }
)
async def get_address_transactions(
    network: str = Path(..., description="The blockchain network identifier", example="torus"),
    address: str = Path(..., description="The blockchain address to query", example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"),
    target_address: Optional[str] = Query(None, description="Filter transactions to/from this address"),
    page: int = Query(1, description="Page number for pagination", ge=1),
    page_size: int = Query(20, description="Number of transactions per page", ge=1, le=100),
    assets: List[str] = Query(
        None,
        description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
        example=["TOR"]
    )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]
    
    try:
        balance_service = BalanceTrackingService(get_clickhouse_connection_string(network))
        result = balance_service.get_address_transactions(address, target_address, page, page_size, assets)

        # No need to convert amounts as they're already in human-readable format

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get(
    "/{network}/balance-tracking/schema",
    summary="Get Balance Tracking Schema",
    description=(
        "Retrieve the schema information for all balance tracking tables. This endpoint "
        "is particularly useful for LLMs (Large Language Models) to understand the structure "
        "of the balance tracking database for generating queries."
    ),
    response_description="Schema information for balance tracking tables",
    responses={
        200: {"description": "Schema retrieved successfully"},
        500: {"description": "Internal server error"}
    },
    tags=["balance-tracking", "mcp"]
)
async def get_balance_tracking_schema(
    network: str = Path(..., description="The blockchain network identifier", example="torus")
):
    try:
        balance_tracking_service = BalanceTrackingService(get_clickhouse_connection_string(network))
        try:
            schema = await balance_tracking_service.get_balance_tracking_schema()
            return schema
        finally:
            balance_tracking_service.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving schema: {str(e)}")

@router.post(
    "/{network}/balance-tracking/query",
    summary="Execute Custom SQL Query",
    description=(
        "Execute a custom SQL query against the balance tracking database. This allows "
        "for advanced data exploration and analysis. Only read-only queries are allowed."
    ),
    response_description="Query results with column names and row count",
    responses={
        200: {"description": "Query executed successfully"},
        400: {"description": "Invalid query syntax"},
        500: {"description": "Internal server error"}
    },
    tags=["balance-tracking", "mcp"]
)
async def execute_balance_tracking_query(
    network: str = Path(..., description="The blockchain network identifier", example="torus"),
    query_params: Dict[str, Any] = Body(
        ...,
        example={
            "query": "SELECT * FROM balance_changes FINAL WHERE address = '5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f' LIMIT 10"
        },
        description="Query parameters including the SQL query to execute"
    )
):
    try:
        # Add network to params
        query_params["network"] = network
        
        # Validate that the query is a SELECT statement
        if not query_params.get("query", "").strip().upper().startswith("SELECT"):
            raise HTTPException(
                status_code=400,
                detail="Only SELECT queries are allowed for security reasons"
            )
            
        # Create service and execute query
        balance_tracking_service = BalanceTrackingService(get_clickhouse_connection_string(network))
        try:
            result = await balance_tracking_service.balance_tracking_query(query_params)
            return result
        finally:
            balance_tracking_service.close()
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Error executing query: {str(e)}")

