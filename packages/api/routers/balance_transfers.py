from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Query, Path, HTTPException, Body

from packages.api.services.balance_tracking_service import BalanceTrackingService
from packages.api.services.balance_transfers_service import BalanceTransferService
from packages.api.tools.balance_transfers import BalanceTransfersTool
from packages.indexers.base import get_clickhouse_connection_string
from packages.indexers.substrate import get_network_asset

router = APIRouter(
    prefix="/substrate",
    tags=["balance-transfers"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"}
    }
)


@router.get(
    "/{network}/balance-transfers/address/{address}",
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
        address: str = Path(..., description="The blockchain address to query",
                            example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"),
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
        balance_service = BalanceTransferService(get_clickhouse_connection_string(network))
        result = balance_service.get_address_transactions(address, target_address, page, page_size, assets)

        # No need to convert amounts as they're already in human-readable format

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get(
    "/{network}/balance-transfers/schema",
    summary="Get Balance Transfers Schema",
    description=(
        "Retrieve the schema information for all balance transfers tables. This endpoint "
        "is particularly useful for LLMs (Large Language Models) to understand the structure "
        "of the balance tracking database for generating queries."
    ),
    response_description="Schema information for balance tracking tables",
    responses={
        200: {"description": "Schema retrieved successfully"},
        500: {"description": "Internal server error"}
    },
    tags=["balance-transfers", "mcp"]
)
async def get_balance_tracking_schema(
    network: str = Path(..., description="The blockchain network identifier", example="torus")
):
    try:
        balance_transfers_tool = BalanceTransfersTool(get_clickhouse_connection_string(network))
        schema = await balance_transfers_tool.schema()
        return schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving schema: {str(e)}")
