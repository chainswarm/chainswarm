from typing import Optional, List
from fastapi import APIRouter, Query, Path, HTTPException, Request
from loguru import logger

from packages.api.services.balance_transfers_service import BalanceTransferService
from packages.indexers.base import get_clickhouse_connection_string
from packages.indexers.substrate import get_native_network_asset
from packages.api.middleware.correlation_middleware import get_request_context, sanitize_params
import os

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
        request: Request,
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
        assets = [get_native_network_asset(network)]

    try:
        balance_service = BalanceTransferService(get_clickhouse_connection_string(network))
        result = balance_service.get_address_transactions(address, target_address, page, page_size, assets, network)

        # No need to convert amounts as they're already in human-readable format

        return result
    except Exception as e:
        logger.error(
            "Balance transfers address transactions query failed",
            error=e,
            operation="get_address_transactions",
            request_context=get_request_context(request),
            query_params=sanitize_params({
                "network": network,
                "address": address,
                "target_address": target_address,
                "page": page,
                "page_size": page_size,
                "assets": assets
            }))

        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get(
    "/{network}/balance-transfers/volume-series",
    summary="Get Balance Transfers Volume Series",
    description=(
        "Retrieves balance transfers volume series data providing network-wide transfer activity metrics.\n\n"
        "This endpoint provides paginated balance transfers volume series data showing network activity "
        "metrics like transaction counts per period, transfer volumes per period, "
        "active addresses per period, and other network activity indicators. "
        "Supports different time aggregations (4-hour, daily, weekly, monthly) and filtering by assets and time range."
    ),
    response_description="Balance transfers volume series data with pagination",
    responses={
        200: {"description": "Balance transfers volume series retrieved successfully"},
        400: {"description": "Invalid period type specified"},
        500: {"description": "Internal server error"}
    }
)
async def get_balance_transfers_volume_series(
        request: Request,
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
            example=1742815440000
        ),
        end_timestamp: Optional[int] = Query(
            None,
            description="End timestamp in milliseconds (Unix timestamp)",
            example=1842815440000
        ),
        period_type: str = Query(
            "4hour",
            description="Period type for aggregation",
            regex="^(4hour|daily|weekly|monthly)$",
            example="4hour"
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_native_network_asset(network)]

    # Validate period_type
    if period_type not in ["4hour", "daily", "weekly", "monthly"]:
        raise HTTPException(status_code=400, detail="Period type must be '4hour', 'daily', 'weekly', or 'monthly'")

    try:
        balance_service = BalanceTransferService(get_clickhouse_connection_string(network))
        result = balance_service.get_balance_volume_series(
            page, page_size, assets, start_timestamp, end_timestamp, period_type, network
        )
        balance_service.close()
        return result
    except Exception as e:

        logger.error(
            "Balance transfers volume series query failed",
            error=e,
            operation="get_balance_transfers_volume_series",
            request_context=get_request_context(request),
            query_params=sanitize_params({
                "network": network,
                "page": page,
                "page_size": page_size,
                "assets": assets,
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "period_type": period_type
            }))
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get(
    "/{network}/balance-transfers/network-analytics/{period}",
    summary="Get Network Analytics",
    description=(
        "Retrieves network analytics for daily, weekly, or monthly periods.\n\n"
        "This endpoint provides network-wide transfer analytics including transaction counts, "
        "volumes, participant metrics, fee statistics, and histogram distributions."
    ),
    response_description="Network analytics data with pagination",
    responses={
        200: {"description": "Network analytics retrieved successfully"},
        400: {"description": "Invalid period specified"},
        500: {"description": "Internal server error"}
    }
)
async def get_network_analytics(
        request: Request,
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        period: str = Path(..., description="Analytics period", regex="^(daily|weekly|monthly)$", example="daily"),
        page: int = Query(1, description="Page number for pagination", ge=1),
        page_size: int = Query(20, description="Number of analytics records per page", ge=1, le=100),
        assets: List[str] = Query(
            None,
            description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
            example=["TOR"]
        ),
        start_date: Optional[str] = Query(
            None,
            description="Start date in YYYY-MM-DD format",
            example="2025-01-01"
        ),
        end_date: Optional[str] = Query(
            None,
            description="End date in YYYY-MM-DD format",
            example="2026-01-31"
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_native_network_asset(network)]

    # Validate period
    if period not in ["daily", "weekly", "monthly"]:
        raise HTTPException(status_code=400, detail="Period must be 'daily', 'weekly', or 'monthly'")

    try:
        balance_service = BalanceTransferService(get_clickhouse_connection_string(network))
        result = balance_service.get_network_analytics(
            period, page, page_size, assets, start_date, end_date
        )
        balance_service.close()
        return result
    except Exception as e:
        logger.error(
            "Network analytics query failed",
            error=e,
            operation="get_network_analytics",
            request_context=get_request_context(request),
            query_params=sanitize_params({
                "network": network,
                "period": period,
                "page": page,
                "page_size": page_size,
                "assets": assets,
                "start_date": start_date,
                "end_date": end_date
            })
        )
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get(
    "/{network}/balance-transfers/address-analytics",
    summary="Get Address Analytics",
    description=(
        "Retrieves comprehensive address analytics with behavioral classification.\n\n"
        "This endpoint provides detailed analytics for addresses including transaction patterns, "
        "volume metrics, behavioral classification, and temporal activity patterns."
    ),
    response_description="Address analytics data with pagination",
    responses={
        200: {"description": "Address analytics retrieved successfully"},
        500: {"description": "Internal server error"}
    }
)
async def get_address_analytics(
        request: Request,
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        page: int = Query(1, description="Page number for pagination", ge=1),
        page_size: int = Query(20, description="Number of address analytics records per page", ge=1, le=100),
        assets: List[str] = Query(
            None,
            description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
            example=["TOR"]
        ),
        address_type: Optional[str] = Query(
            None,
            description="Filter by address type classification",
            example="Exchange"
        ),
        min_volume: Optional[float] = Query(
            None,
            description="Minimum total volume filter",
            example=1000.0
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_native_network_asset(network)]

    try:
        balance_service = BalanceTransferService(get_clickhouse_connection_string(network))
        result = balance_service.get_address_analytics(
            page, page_size, assets, address_type, min_volume
        )
        balance_service.close()
        return result
    except Exception as e:
        logger.error(
            "Address analytics query failed",
            error=e,
            operation="get_address_analytics",
            request_context=get_request_context(request),
            query_params=sanitize_params({
                "network": network,
                "page": page,
                "page_size": page_size,
                "assets": assets,
                "address_type": address_type,
                "min_volume": min_volume
            })
        )
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get(
    "/{network}/balance-transfers/volume-aggregations/{period}",
    summary="Get Volume Aggregations",
    description=(
        "Retrieves volume aggregations for daily, weekly, or monthly periods.\n\n"
        "This endpoint provides aggregated volume data with transaction counts, "
        "histogram distributions, and statistical measures."
    ),
    response_description="Volume aggregations data with pagination",
    responses={
        200: {"description": "Volume aggregations retrieved successfully"},
        400: {"description": "Invalid period specified"},
        500: {"description": "Internal server error"}
    }
)
async def get_volume_aggregations(
        request: Request,
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        period: str = Path(..., description="Aggregation period", regex="^(daily|weekly|monthly)$", example="daily"),
        page: int = Query(1, description="Page number for pagination", ge=1),
        page_size: int = Query(20, description="Number of aggregation records per page", ge=1, le=100),
        assets: List[str] = Query(
            None,
            description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
            example=["TOR"]
        ),
        start_date: Optional[str] = Query(
            None,
            description="Start date in YYYY-MM-DD format",
            example="2025-01-01"
        ),
        end_date: Optional[str] = Query(
            None,
            description="End date in YYYY-MM-DD format",
            example="2026-01-31"
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_native_network_asset(network)]

    # Validate period
    if period not in ["daily", "weekly", "monthly"]:
        raise HTTPException(status_code=400, detail="Period must be 'daily', 'weekly', or 'monthly'")

    try:
        balance_service = BalanceTransferService(get_clickhouse_connection_string(network))
        result = balance_service.get_volume_aggregations(
            period, page, page_size, assets, start_date, end_date
        )
        balance_service.close()
        return result
    except Exception as e:
        logger.error(
            "Volume aggregations query failed",
            error=e,
            operation="get_volume_aggregations",
            request_context=get_request_context(request),
            query_params=sanitize_params({
                "network": network,
                "period": period,
                "page": page,
                "page_size": page_size,
                "assets": assets,
                "start_date": start_date,
                "end_date": end_date
            })
        )
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get(
    "/{network}/balance-transfers/volume-trends",
    summary="Get Volume Trends",
    description=(
        "Retrieves volume trends with rolling averages for trend analysis.\n\n"
        "This endpoint provides volume trends data with 7-period and 30-period "
        "rolling averages for identifying patterns and trends in network activity."
    ),
    response_description="Volume trends data with pagination",
    responses={
        200: {"description": "Volume trends retrieved successfully"},
        500: {"description": "Internal server error"}
    }
)
async def get_volume_trends(
        request: Request,
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        page: int = Query(1, description="Page number for pagination", ge=1),
        page_size: int = Query(20, description="Number of trend records per page", ge=1, le=100),
        assets: List[str] = Query(
            None,
            description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
            example=["TOR"]
        ),
        start_timestamp: Optional[int] = Query(
            None,
            description="Start timestamp in milliseconds (Unix timestamp)",
            example=1740995200000
        ),
        end_timestamp: Optional[int] = Query(
            None,
            description="End timestamp in milliseconds (Unix timestamp)",
            example=1840995200000
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_native_network_asset(network)]

    try:
        balance_service = BalanceTransferService(get_clickhouse_connection_string(network))
        result = balance_service.get_volume_trends(
            page, page_size, assets, start_timestamp, end_timestamp
        )
        balance_service.close()
        return result
    except Exception as e:
        logger.error(
            "Volume trends query failed",
            error=e,
            operation="get_volume_trends",
            request_context=get_request_context(request),
            query_params=sanitize_params({
                "network": network,
                "page": page,
                "page_size": page_size,
                "assets": assets,
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp
            })
        )
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get(
    "/{network}/balance-transfers/addresses/analytics",
    summary="Get Analytics for Multiple Addresses",
    description=(
        "Retrieves comprehensive analytics for a list of addresses from the balance transfers system.\n\n"
        "This endpoint provides detailed analytics including volume in/out, transaction patterns, "
        "counterparty analysis, time-based activity patterns, volume distributions, and address "
        "classification for each address-asset combination. Perfect for bulk address analysis.\n\n"
        "Use `return_all=true` to get all results without pagination."
    ),
    response_description="Comprehensive address analytics data with pagination or all results",
    responses={
        200: {"description": "Address analytics retrieved successfully"},
        400: {"description": "Invalid input parameters"},
        500: {"description": "Internal server error"}
    }
)
async def get_addresses_analytics(
        request: Request,
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        addresses: List[str] = Query(
            ...,
            description="List of blockchain addresses to analyze",
            example=["5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f", "5FZdduraHpWTVFBehbH4yqsfi7LabXFQkmqc2vKqbQTaspwM"]
        ),
        page: int = Query(1, description="Page number for pagination (ignored if return_all=true)", ge=1),
        page_size: int = Query(20, description="Number of analytics records per page (ignored if return_all=true)", ge=1, le=100),
        assets: List[str] = Query(
            None,
            description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
            example=["TOR"]
        ),
        return_all: bool = Query(
            False,
            description="If true, returns all results without pagination. Ignores page and page_size parameters.",
            example=False
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_native_network_asset(network)]

    # Validate addresses parameter
    if not addresses:
        raise HTTPException(status_code=400, detail="At least one address must be provided")

    try:
        balance_service = BalanceTransferService(get_clickhouse_connection_string(network))
        result = balance_service.get_addresses_analytics(addresses, page, page_size, assets, return_all, network)
        balance_service.close()
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(
            "Addresses analytics query failed",
            error=e,
            operation="get_addresses_analytics",
            request_context=get_request_context(request),
            query_params=sanitize_params({
                "network": network,
                "addresses": addresses,
                "page": page,
                "page_size": page_size,
                "assets": assets,
                "return_all": return_all
            }))

        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

