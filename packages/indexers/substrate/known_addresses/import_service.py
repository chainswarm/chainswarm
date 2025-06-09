import os
import sys
import json
import traceback
import uuid
import time
import requests
from typing import List, Dict, Any, Set
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv
from clickhouse_connect import get_client

from packages.indexers.base import setup_logger, terminate_event, get_clickhouse_connection_string

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


class KnownAddressesImportService:
    """
    Standalone service for importing known addresses from external sources
    """
    
    def __init__(self, clickhouse_params: Dict[str, Any]):
        """
        Initialize the KnownAddressesImportService
        
        Args:
            clickhouse_params: ClickHouse connection parameters
        """
        self.clickhouse_params = clickhouse_params
        self.client = get_client(
            host=clickhouse_params['host'],
            port=int(clickhouse_params['port']),
            username=clickhouse_params['user'],
            password=clickhouse_params['password'],
            database=clickhouse_params['database']
        )
        # Default URL for Torus network if no other URL is specified
        self.repo_url = os.getenv("KNOWN_ADDRESSES_REPO_URL", "")
        self._ensure_tables_exist()
    
    def _ensure_tables_exist(self):
        """Ensure the required tables exist in ClickHouse"""
        try:
            # Create the known_addresses table if it doesn't exist with versioning
            self.client.command("""
                CREATE TABLE IF NOT EXISTS known_addresses (
                    id UUID,
                    network String,
                    address String,
                    label String,
                    source String,
                    source_type String,
                    last_updated DateTime,
                    _version UInt64,
                    PRIMARY KEY (network, address, source)
                )
                ENGINE = ReplacingMergeTree(_version)
                ORDER BY (network, address, source)
            """)
            
            logger.info("Known addresses table created or already exists")
        except Exception as e:
            logger.error(f"Error creating known_addresses table: {str(e)}")
            raise
    
    def import_from_repo(self, network: str) -> bool:
        """
        Import known addresses from repository and remove old entries
        
        Args:
            network: The blockchain network identifier
            
        Returns:
            True if import was successful, False otherwise
        """
        if not self.repo_url:
            logger.error("Repository URL not configured")
            return False
        
        try:
            # Construct the URL for the specific network
            url = f"{self.repo_url.rstrip('/')}/{network}_known_addresses.json"
            logger.info(f"Fetching known addresses from {url}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            addresses_data = response.json()
            
            if not addresses_data:
                logger.warning(f"No addresses found for network {network}")
                return False
            
            # Get current timestamp for versioning
            current_version = int(datetime.now().timestamp())
            
            # Insert the addresses into ClickHouse with versioning
            rows = []
            imported_addresses = set()  # Track addresses being imported
            
            for item in addresses_data:
                address = item.get("address", "")
                source = item.get("source", "repo")
                
                # Add to tracking set for later cleanup
                imported_addresses.add((address, source))
                
                rows.append({
                    "id": str(uuid.uuid4()),  # Generate UUID for new entries
                    "network": network,
                    "address": address,
                    "label": item.get("label", ""),
                    "source": source,
                    "source_type": item.get("source_type", "external"),
                    "last_updated": datetime.now(),  # Set current timestamp
                    "_version": current_version  # Use timestamp as version
                })
            
            if rows:
                # Insert new data - convert dictionary rows to a list of lists
                column_names = ["id", "network", "address", "label", "source", "source_type", "last_updated", "_version"]
                data = []
                for row in rows:
                    data.append([
                        row["id"],
                        row["network"],
                        row["address"],
                        row["label"],
                        row["source"],
                        row["source_type"],
                        row["last_updated"],
                        row["_version"]
                    ])
                
                self.client.insert("known_addresses", data, column_names=column_names)
                logger.info(f"Imported {len(rows)} known addresses for network {network}")
                
                # Remove old entries that are not in the current import
                self._remove_old_entries(network, imported_addresses, current_version)
                
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error importing known addresses from repository: {str(e)}", trb=traceback.format_exc(),)
            return False
    
    def _remove_old_entries(self, network: str, imported_addresses: set, current_version: int):
        """
        Remove entries that are no longer in the import file
        
        Args:
            network: The blockchain network identifier
            imported_addresses: Set of (address, source) tuples that were imported
            current_version: Current version timestamp
        """
        try:
            # Get all existing addresses for this network
            query = """
                SELECT address, source
                FROM known_addresses FINAL
                WHERE network = {network:String}
            """
            
            result = self.client.query(query, parameters={"network": network})
            
            # Find addresses to remove (those in DB but not in imported set)
            addresses_to_remove = []
            for row in result.result_rows:
                address = row[0]
                source = row[1]
                
                if (address, source) not in imported_addresses:
                    addresses_to_remove.append((address, source))
            
            if addresses_to_remove:
                logger.info(f"Removing {len(addresses_to_remove)} old entries for network {network}")
                
                # Actually delete the old entries using ALTER TABLE DELETE
                for address, source in addresses_to_remove:
                    delete_query = """
                        ALTER TABLE known_addresses
                        DELETE WHERE
                            network = {network:String} AND
                            address = {address:String} AND
                            source = {source:String}
                    """
                    
                    try:
                        self.client.command(
                            delete_query,
                            parameters={
                                "network": network,
                                "address": address,
                                "source": source
                            }
                        )
                    except Exception as e:
                        logger.error(f"Error deleting entry {address}/{source}: {str(e)}")
                
                logger.info(f"Deleted {len(addresses_to_remove)} old entries for network {network}")
        except Exception as e:
            logger.error(f"Error removing old entries: {str(e)}")


async def main():
    """Main entry point"""
    # Load environment variables
    load_dotenv()
    
    # Setup logger
    setup_logger("known-addresses-import-service")
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Known Addresses Import Service")
    parser.add_argument("--networks", type=str, required=True, help="Comma-separated list of networks to import")
    parser.add_argument("--sleep-interval", type=int, default=3600, help="Sleep interval in seconds after import (default: 3600)")
    args = parser.parse_args()
    
    # Parse networks
    networks = [n.strip() for n in args.networks.split(",")]
    sleep_interval = args.sleep_interval
    
    logger.info(f"Starting import service for networks: {', '.join(networks)}")
    
    while not terminate_event.is_set():
        try:
            for network in networks:
                if terminate_event.is_set():
                    break
                    
                # Get ClickHouse connection parameters
                clickhouse_params = get_clickhouse_connection_string(network)
                
                # Initialize the import service
                import_service = KnownAddressesImportService(clickhouse_params)
                
                # Import the addresses
                success = import_service.import_from_repo(network)
                
                if success:
                    logger.info(f"Successfully imported known addresses for network {network}")
                else:
                    logger.warning(f"No addresses imported for network {network}")
            
            # Sleep until next import cycle or termination
            logger.info(f"Import cycle completed. Sleeping for {sleep_interval} seconds")
            
            # Sleep in small intervals to check for termination
            for _ in range(sleep_interval):
                if terminate_event.is_set():
                    break
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in import cycle: {str(e)}")
            if not terminate_event.is_set():
                # Sleep for a short time before retrying
                time.sleep(10)
    
    logger.info("Import service terminating gracefully")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())