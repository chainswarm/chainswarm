from typing import Annotated, Optional, List
from fastapi import APIRouter, Query, Path, HTTPException
from packages.api.routers import get_memgraph_driver
from packages.api.services.balance_series_service import BalanceSeriesService
from packages.api.services.balance_transfers_service import BalanceTransferService
from packages.api.services.money_flow_service import MoneyFlowService, Direction
from packages.indexers.base import get_clickhouse_connection_string
from packages.api.utils.money_flow_utils import add_searched_badge_to_nodes, enrich_nodes_with_balances
from packages.indexers.substrate import get_network_asset

router = APIRouter(
    prefix="/substrate",
    tags=["money-flow"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"}
    }
)

@router.get(
    "/{network}/money-flow/path/shortest",
    summary="Retrieve the shortest path money flow between two addresses",
    description=(
            "Retrieves the shortest path between two addresses on a specified blockchain network.\n\n"
            "**Traversal Behavior:**"
            "The system will perform a **Breadth-First Search (BFS)** to find the shortest path "
            "between the starting address and the target address."
    ),
    response_description="List of the money flows or a message indicating no flows found.",
    responses={
        200: {"description": "Money flows retrieved successfully"},
        400: {"description": "Invalid input parameters"},
        404: {"description": "No money flow found"},
        500: {"description": "Internal server error"}
    }
)
async def get_money_flow_by_path_shortest(
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        source_address: str = Query(
            ...,
            description="The source wallet address list",
            example="5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"
        ),
        target_address: Optional[str] = Query(
            ...,
            description="The target address to query",
            example="5FZdduraHpWTVFBehbH4yqsfi7LabXFQkmqc2vKqbQTaspwM"
        ),
        assets: List[str] = Query(
            None,
            description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
            example=["TOR"]
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]
    
    memgraph_driver = get_memgraph_driver(network)
    try:
        money_flow_service = MoneyFlowService(memgraph_driver)
        result = money_flow_service.get_money_flow_by_path_shortest(
            source_address=source_address,
            target_address=target_address,
            assets=assets
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    if not result:
        return []
        
    result = add_searched_badge_to_nodes(result, [source_address, target_address])
    clickhouse_params = get_clickhouse_connection_string(network)
    balance_series_service = BalanceSeriesService(clickhouse_params)
    result = enrich_nodes_with_balances(balance_series_service, result, network, assets)

    return result


@router.get(
    "/{network}/money-flow/path/explore/address",
    summary="Retrieve money flows for addresses",
    description=(
            "Retrieves the money flows for addresses on a specified blockchain network with filtering options to reduce noise.\n\n"
            "**Traversal Behavior:**\n"
            "The system will perform a **Depth-First Search (DFS)** and return every discovered path.\n\n"
    ),
    response_description="List of the money flows or a message indicating no flows found.",
    responses={
        200: {"description": "Money flows retrieved successfully"},
        400: {"description": "Invalid input parameters"},
        404: {"description": "No money flow found"},
        500: {"description": "Internal server error"}
    }
)
async def get_money_flow_by_path_explore_address(
        network: str = Path(..., description="The blockchain network identifier", example="torus"),
        addresses: List[str] = Query(
            ...,
            description="The wallet address list",
            example=["5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f"]
        ),
        depth_level: int = Query(3, description="The depth level to explore", example=3),
        direction: Direction = Query(Direction.out_, description="The direction of the money flow", example="all"),
        assets: List[str] = Query(
            None,
            description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
            example=["TOR"]
        )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]
    
    memgraph_driver = get_memgraph_driver(network)
    try:
        money_flow_service = MoneyFlowService(memgraph_driver)
        result = money_flow_service.get_money_flow_by_path_explore(
            addresses,
            depth_level,
            direction,
            assets
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

    if not result:
        return []

    result = add_searched_badge_to_nodes(result, addresses)

    clickhouse_params = get_clickhouse_connection_string(network)
    balance_series_service = BalanceSeriesService(clickhouse_params)
    result = enrich_nodes_with_balances(balance_series_service, result, network, assets)

    return result

@router.get(
    "/{network}/money-flow/path/explore/transaction",
    summary="Retrieve money flows by Transaction ID",
    description=(
            "Retrieves the money flows for addresses involved in a transaction on a specified blockchain network.\n\n"
            "**Traversal Behavior:**"
            "The system will perform a **Depth-First Search (DFS)** and return every discovered path."
    ),
    response_description="List of the money flows or a message indicating no flows found.",
    responses={
        200: {"description": "Money flows retrieved successfully"},
        400: {"description": "Invalid input parameters"},
        404: {"description": "No money flow found"},
        500: {"description": "Internal server error"}
    }
)
async def get_money_flow_by_path_explore_transaction_id(
    network: Annotated[str, Path(description="The blockchain network identifier", example="torus")],
    transaction_id: str = Query(description="The transaction ID", example="308-0001"),
    depth_level: int = Query(3, description="The depth level to explore", example=3),
    assets: List[str] = Query(
        None,
        description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
        example=["TOR"]
    )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]
    
    block_height, padded_idx = transaction_id.split("-")
    un_padded_idx = str(int(padded_idx))
    extrinsic_id = f"{block_height}-{un_padded_idx}"

    clickhouse_conn = get_clickhouse_connection_string(network)
    balance_transfers_service = BalanceTransferService(clickhouse_conn)
    addresses = balance_transfers_service.get_addresses_from_transaction_id(extrinsic_id, assets)

    memgraph_driver = get_memgraph_driver(network)
    try:
        money_flow_service = MoneyFlowService(memgraph_driver)
        result = money_flow_service.get_money_flow_by_path_explore(addresses, depth_level, Direction.all_, assets)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    if not result:
        return []

    result = add_searched_badge_to_nodes(result, addresses)

    clickhouse_params = get_clickhouse_connection_string(network)
    balance_series_service = BalanceSeriesService(clickhouse_params)
    result = enrich_nodes_with_balances(balance_series_service, result, network, assets)

    return result

@router.get(
    "/{network}/money-flow/path/explore/block",
    summary="Retrieve Recent Money Flows by Block Height",
    description=(
            "Retrieves the most recent money flows between addresses on a specified blockchain network.\n\n"
            "**Traversal Behavior:**"
            "The system will perform a **Depth-First Search (DFS)** and return every discovered path."
    ),
    response_description="List of recent money flows or a message indicating no flows found.",
    responses={
        200: {"description": "Money flows retrieved successfully"},
        400: {"description": "Invalid input parameters"},
        404: {"description": "No money flow found"},
        500: {"description": "Internal server error"}
    }
)
async def get_money_flow_by_block(
    network: Annotated[str, Path(description="The blockchain network identifier", example="torus")],
    block_height: int = Query(description="The block height", example=308),
    depth_level: int = Query(3, description="The depth level to explore", example=3),
    assets: List[str] = Query(
        None,
        description="List of assets to filter by. Use ['all'] for all assets. Defaults to network's native asset.",
        example=["TOR"]
    )
):
    # Handle assets parameter - default to network's native asset if not provided
    if assets is None:
        assets = [get_network_asset(network)]
    
    balance_transfer_service = BalanceTransferService(get_clickhouse_connection_string(network))
    addresses = balance_transfer_service.get_addresses_from_block_height(block_height, assets)

    memgraph_driver = get_memgraph_driver(network)
    try:
        money_flow_service = MoneyFlowService(memgraph_driver)
        result = money_flow_service.get_money_flow_by_path_explore(addresses, depth_level, Direction.all_, assets)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    if not result:
        return []

    result = add_searched_badge_to_nodes(result, addresses)
    clickhouse_params = get_clickhouse_connection_string(network)
    balance_series_service = BalanceSeriesService(clickhouse_params)
    result = enrich_nodes_with_balances(balance_series_service, result, network, assets)

    return result
