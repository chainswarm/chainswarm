import os
import uuid
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional
from loguru import logger
from clickhouse_connect import get_client

class KnownAddressesImporter:
    """
    Class for importing known addresses from GitHub repository
    """
    
    def __init__(self, clickhouse_params: Dict[str, Any]):
        """
        Initialize the KnownAddressesImporter
        
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
        self.github_repo_url = os.getenv("KNOWN_ADDRESSES_GITHUB_URL", "")
        self.update_interval_seconds = int(os.getenv("KNOWN_ADDRESSES_UPDATE_INTERVAL", "3600"))  # Default: 1 hour
        
    def ensure_tables_exist(self):
        """
        Ensure the required tables exist in ClickHouse
        """
        try:
            # Load schema from SQL file
            schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            
            # Execute each CREATE TABLE statement separately
            # Split by semicolon and filter out empty statements
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            for statement in statements:
                if 'CREATE TABLE' in statement:
                    self.client.command(statement)
            
            logger.info("Known addresses tables created or already exist")
            return True
        except Exception as e:
            logger.error(f"Error creating known_addresses tables: {str(e)}")
            return False
    
    def import_from_github(self, network: str) -> bool:
        """
        Import known addresses from GitHub repository
        
        Args:
            network: The blockchain network identifier
            
        Returns:
            True if import was successful, False otherwise
        """
        if not self.github_repo_url:
            logger.error("GitHub repository URL not configured")
            return False
        
        try:
            # Ensure tables exist
            if not self.ensure_tables_exist():
                return False
                
            # Construct the URL for the specific network
            url = f"{self.github_repo_url.rstrip('/')}/{network}_known_addresses.json"
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
            for item in addresses_data:
                rows.append({
                    "id": str(uuid.uuid4()),  # Generate UUID for new entries
                    "network": network,
                    "address": item.get("address", ""),
                    "label": item.get("label", ""),
                    "source": item.get("source", "github"),
                    "source_type": item.get("source_type", "external"),  # Default to "external"
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
                
                # Update the last update time - ClickHouse doesn't support ON CONFLICT
                # First delete any existing record
                try:
                    self.client.command(
                        """
                        ALTER TABLE known_addresses_updates
                        DELETE WHERE network = {network:String}
                        """,
                        parameters={"network": network}
                    )
                except Exception as e:
                    logger.warning(f"Error deleting existing update record: {str(e)}")
                
                # Then insert the new record
                self.client.command(
                    """
                    INSERT INTO known_addresses_updates (network, last_updated)
                    VALUES ({network:String}, now())
                    """,
                    parameters={"network": network}
                )
                
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error importing known addresses from GitHub: {str(e)}")
            return False