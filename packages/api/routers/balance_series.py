from typing import Optional, List
from fastapi import APIRouter, Query, Path, HTTPException
from packages.api.services.balance_series_service import BalanceSeriesService
from packages.indexers.base import get_clickhouse_connection_string
from packages.indexers.substrate import get_network_asset

router = APIRouter(
    prefix="/substrate",
    tags=["balance-series"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"}
    }
)


@router.get(
    "/{network}/balance-series/address/{address}/history",
    summary="Get Address Balance History",
    description=(
        "Retrieves historical balance snapshots for a specific address on a blockchain network.\n\n"
        "This endpoint provides a paginated list of balance snapshots at fixed 4-hour intervals, "
        "showing free, reserved, staked, and total balances along with changes between periods. "
        "Optionally filter by time range and assets."
    ),
    response_description="Address balance history with pagination",
    responses={
        200: {"description": "Balance history retrieved successfully"},
        404: {"description": "Address not found"},
        500: {"description": "Internal server error"}
    }
)
async def get_address_balance_history(
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        address: str = Path(..., description="The blockchain address to query",
                            example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"),
        page: int = Query(1, description="Page number for pagination", ge=1),
        page_size: int = Query(20, description="Number of balance snapshots per page", ge=1, le=100),
        assets: List[str] = Query(
            None,
            description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
            example=["TOR"]
        ),
        start_timestamp: Optional[int] = Query(
            None,
            description="Start timestamp in milliseconds (Unix timestamp)",
            example=1640995200000
        ),
        end_timestamp: Optional[int] = Query(
            None,
            description="End timestamp in milliseconds (Unix timestamp)",
            example=1641081600000
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]

    try:
        balance_service = BalanceSeriesService(get_clickhouse_connection_string(network))
        result = balance_service.get_address_balance_series(
            address, page, page_size, assets, start_timestamp, end_timestamp
        )
        balance_service.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get(
    "/{network}/balance-series/address/{address}/current",
    summary="Get Current Address Balance",
    description=(
        "Retrieves the latest balance snapshot for a specific address on a blockchain network.\n\n"
        "This endpoint returns the most recent balance data including free, reserved, staked, "
        "and total balances for the specified address and assets."
    ),
    response_description="Current balance for the address",
    responses={
        200: {"description": "Current balance retrieved successfully"},
        404: {"description": "Address not found"},
        500: {"description": "Internal server error"}
    }
)
async def get_address_current_balance(
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        address: str = Path(..., description="The blockchain address to query",
                            example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"),
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
        balance_service = BalanceSeriesService(get_clickhouse_connection_string(network))
        result = balance_service.get_current_balances([address], assets)
        balance_service.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get(
    "/{network}/balance-series/address/{address}/changes",
    summary="Get Address Balance Changes",
    description=(
        "Retrieves balance changes analysis for a specific address on a blockchain network.\n\n"
        "This endpoint provides a paginated list of balance changes, showing periods where "
        "the balance changed along with the magnitude of changes. Results are ordered by "
        "change magnitude (largest changes first)."
    ),
    response_description="Address balance changes with pagination",
    responses={
        200: {"description": "Balance changes retrieved successfully"},
        404: {"description": "Address not found"},
        500: {"description": "Internal server error"}
    }
)
async def get_address_balance_changes(
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        address: str = Path(..., description="The blockchain address to query",
                            example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"),
        page: int = Query(1, description="Page number for pagination", ge=1),
        page_size: int = Query(20, description="Number of balance changes per page", ge=1, le=100),
        assets: List[str] = Query(
            None,
            description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
            example=["TOR"]
        ),
        min_change_threshold: Optional[float] = Query(
            None,
            description="Minimum absolute change threshold to filter by (in token units)",
            example=1000.0
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]

    try:
        balance_service = BalanceSeriesService(get_clickhouse_connection_string(network))
        result = balance_service.get_balance_changes(
            address, page, page_size, assets, min_change_threshold
        )
        balance_service.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get(
    "/{network}/balance-series/aggregations/{period}",
    summary="Get Balance Aggregations",
    description=(
        "Retrieves balance aggregations for daily, weekly, or monthly periods.\n\n"
        "This endpoint provides aggregated balance data over different time periods, "
        "showing end-of-period balances and cumulative changes. Useful for trend analysis "
        "and reporting across multiple addresses and assets."
    ),
    response_description="Balance aggregations for the specified period",
    responses={
        200: {"description": "Balance aggregations retrieved successfully"},
        400: {"description": "Invalid period specified"},
        500: {"description": "Internal server error"}
    }
)
async def get_balance_aggregations(
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        period: str = Path(..., description="Aggregation period", regex="^(daily|weekly|monthly)$", example="daily"),
        addresses: List[str] = Query(
            None,
            description="List of addresses to filter by. If not provided, returns data for all addresses.",
            example=["5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"]
        ),
        assets: List[str] = Query(
            None,
            description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
            example=["TOR"]
        ),
        start_date: Optional[str] = Query(
            None,
            description="Start date in YYYY-MM-DD format",
            example="2024-01-01"
        ),
        end_date: Optional[str] = Query(
            None,
            description="End date in YYYY-MM-DD format",
            example="2024-01-31"
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]

    # Validate period
    if period not in ["daily", "weekly", "monthly"]:
        raise HTTPException(status_code=400, detail="Period must be 'daily', 'weekly', or 'monthly'")

    try:
        balance_service = BalanceSeriesService(get_clickhouse_connection_string(network))
        result = balance_service.get_balance_aggregations(
            period, addresses, assets, start_date, end_date
        )
        balance_service.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get(
    "/{network}/balance-series/volume-series",
    summary="Get Balance Volume Series",
    description=(
        "Retrieves network-wide balance activity volume metrics over time.\n\n"
        "This endpoint provides aggregated balance activity data showing the volume of balance changes "
        "across all addresses for each time period. It includes metrics like active addresses count, "
        "total balance change volumes, and breakdowns by balance type (free, reserved, staked). "
        "Results are grouped by time periods and assets, useful for analyzing network-wide balance activity trends."
    ),
    response_description="Balance volume series with pagination",
    responses={
        200: {"description": "Balance volume series retrieved successfully"},
        500: {"description": "Internal server error"}
    }
)
async def get_balance_volume_series(
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        page: int = Query(1, description="Page number for pagination", ge=1),
        page_size: int = Query(20, description="Number of volume series records per page", ge=1, le=100),
        assets: List[str] = Query(
            None,
            description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
            example=["TOR"]
        ),
        start_timestamp: Optional[int] = Query(
            None,
            description="Start timestamp in milliseconds (Unix timestamp)",
            example=1640995200000
        ),
        end_timestamp: Optional[int] = Query(
            None,
            description="End timestamp in milliseconds (Unix timestamp)",
            example=1641081600000
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]

    try:
        balance_service = BalanceSeriesService(get_clickhouse_connection_string(network))
        result = balance_service.get_balance_volume_series(
            page, page_size, assets, start_timestamp, end_timestamp
        )
        balance_service.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
