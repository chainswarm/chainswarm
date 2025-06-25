import json
import argparse
import traceback
from loguru import logger
from neo4j import GraphDatabase
from packages.indexers.base import setup_logger, get_memgraph_connection_string
from packages.indexers.base.decimal_utils import convert_to_decimal_units
from packages.indexers.substrate import get_network_asset


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

    run(args.file, args.network)


def run(file: str, network: str):
    try:
        logger.info(f"Loading genesis balances from {file}")
        balances = load_genesis_balances(file)
        logger.info(f"Loaded {len(balances)} genesis balances")
        
        # Get the asset for this network
        asset = get_network_asset(network)
        logger.info(f"Using asset: {asset} for network: {network}")
        
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

                # Skip if genesis addresses already exist for this asset
                if result and result.get('addr'):
                    logger.info(f"Genesis addresses already exist for asset {asset} - skipping insertion")
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
                        'asset': asset,
                        'block_height': 0,
                        'timestamp': timestamp,
                        'amount': float(convert_to_decimal_units(
                            amount,
                            network
                        ))
                    })

        logger.info(f"Genesis balances populated successfully for asset: {asset}")

    except Exception as e:
        logger.error(f"Error populating genesis balances", error=e, trb=traceback.format_exc())
        raise e


if __name__ == "__main__":
    main()