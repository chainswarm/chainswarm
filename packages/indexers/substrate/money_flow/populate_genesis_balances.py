import json
import argparse
import traceback
from loguru import logger
from neo4j import GraphDatabase
from packages.indexers.base import setup_logger, get_memgraph_connection_string, get_clickhouse_connection_string
from packages.indexers.base.decimal_utils import convert_to_decimal_units
from packages.indexers.substrate import Network
from packages.indexers.substrate.assets.asset_manager import AssetManager


def load_genesis_balances(file_path: str):
    """Load genesis balances from JSON file"""
    with open(file_path, 'r') as f:
        return json.load(f)


timestamp = 1735946640000


def main():
    parser = argparse.ArgumentParser(description='Populate genesis balances from JSON file')
    parser.add_argument(
        '--network',
        type=str,
        required=True,
        help='Network to populate balances for (e.g., torus)'
    )
    parser.add_argument(
        '--file',
        type=str,
        default='data/torus-genesis-balances.json',
        help='Path to genesis balances JSON file'
    )
    args = parser.parse_args()
    service_name = f'substrate-{args.network}-populate-genesis-money-flow'
    setup_logger(service_name)

    # Create AssetManager
    clickhouse_params = get_clickhouse_connection_string(args.network)
    asset_manager = AssetManager(args.network, clickhouse_params)
    asset_manager.init_tables()
    
    run(args.file, args.network, asset_manager)


def run(file: str, network: str, asset_manager: AssetManager):
    try:
        logger.info(f"Loading genesis balances from {file}")
        balances = load_genesis_balances(file)
        logger.info(f"Loaded {len(balances)} genesis balances")
        asset_symbol = "TOR"

        # Ensure native asset exists
        decimals = asset_manager.NATIVE_ASSETS.get(network, {}).get('decimals', 0)
        asset_manager.ensure_asset_exists(
            asset_symbol=asset_symbol,
            asset_contract='native',
            asset_type='native',
            decimals=decimals
        )
        
        graph_db_url, graph_db_user, graph_db_password = get_memgraph_connection_string(network)

        graph_database = GraphDatabase.driver(
            graph_db_url,
            auth=(graph_db_user, graph_db_password),
            max_connection_lifetime=3600,
            connection_acquisition_timeout=60
        )
        with graph_database.session() as session:
            with session.begin_transaction() as tx:
                # Check if any genesis addresses exist for this asset
                result = tx.run("""
                        MATCH (addr:Address:Genesis)
                        RETURN addr LIMIT 1
                    """).single()

                # Skip if genesis addresses already exist
                if result and result.get('addr'):
                    logger.info(f"Genesis addresses already exist - skipping insertion")
                    return

                for i, (address, amount) in enumerate(balances):
                    query = """
                        MERGE (addr:Address:Genesis {address: $account, id: $account })
                        SET
                            addr.first_transfer_timestamp = $timestamp,
                            addr.labels = ['genesis']
                        """
                    tx.run(query, {
                        'event_id': f"genesis-{i}",
                        'account': address,
                        'block_height': 0,
                        'timestamp': timestamp,
                        'amount': float(convert_to_decimal_units(
                            amount,
                            network
                        ))
                    })

        logger.info(f"Genesis balances populated successfully for asset: {asset_symbol}")

    except Exception as e:
        logger.error(
            "Error populating genesis balances",
            error=e,
            traceback=traceback.format_exc(),
            extra={
                "file": file,
                "network": network,
                "asset": asset_symbol if 'asset_symbol' in locals() else None
            }
        )
        raise


if __name__ == "__main__":
    main()