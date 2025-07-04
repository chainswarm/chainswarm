
from fastapi import APIRouter, Query, Path, HTTPException
from typing import Optional
from packages.api.services.assets_service import AssetsService
from packages.indexers.base import get_clickhouse_connection_string
from packages.api.middleware.correlation_middleware import get_request_context, sanitize_params
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/substrate",
    tags=["assets"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"}
    }
)


@router.get(
    "/{network}/assets",
    summary="Get all assets without pagination",
    description=(
        "Returns all assets from the assets dictionary table without pagination. "
        "This endpoint provides complete asset information including symbols, contracts, "
        "verification status, and metadata. Optionally filter by network."
    )
)
async def get_all_assets(
    network: str = Path(..., description="The blockchain network identifier", example="torus")
):
    try:
        connection_params = get_clickhouse_connection_string(network)
        
        assets_service = AssetsService(connection_params)
        result = assets_service.get_assets(network=network)

        return result
    except Exception as e:
        logger.error(f"Error getting all assets: {str(e)}", extra=get_request_context())
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
