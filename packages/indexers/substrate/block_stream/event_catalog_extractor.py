import argparse
import json
import os
import time
import traceback
from loguru import logger
from typing import Dict, Any

from packages.indexers.base import setup_logger, get_clickhouse_connection_string, create_clickhouse_database, terminate_event
from packages.indexers.substrate import networks
from packages.indexers.substrate.block_stream.block_stream_manager import BlockStreamManager


class EventCatalogExtractor:
    """
    Extracts unique module_id to event_id pairs with example attribute fields from the block_stream table.
    
    This class processes blocks in batches, extracts unique module_id/event_id pairs with their attributes,
    and writes them to a file. It uses a buffering strategy where it processes a batch of blocks,
    writes the catalog to file, and then reloads from file before processing the next batch.
    """
    
    def __init__(
            self,
            connection_params: Dict[str, Any],
            network: str,
            batch_size: int = 100,
            output_file: str = None,
            service_name: str = None,
            continuous: bool = False
    ):
        """
        Initialize the EventCatalogExtractor.
        
        Args:
            connection_params: Dictionary containing ClickHouse connection parameters
            network: Network name (e.g., 'torus', 'polkadot', 'bittensor')
            batch_size: Number of blocks to process in a batch
            output_file: Output file path (default: event_catalog_<network>.txt)
            service_name: Service name for logging
        """
        # Initialize connection
        self.block_stream_manager = BlockStreamManager(connection_params)
        self.network = network
        self.batch_size = batch_size
        self.output_file = output_file or f"event_catalog_{network}.txt"
        self.service_name = service_name or f'substrate-{network}-event-catalog'
        self.continuous = continuous
        self.event_catalog = {}
        self.processed_blocks = 0
        self.last_processed_height = 0
        
    def run(self):
        """
        Main processing loop.
        
        This method:
        1. Gets the latest block height from the database
        2. Loads any existing catalog from file
        3. Processes blocks in batches
        4. Writes the catalog to file after each batch
        5. Reloads the catalog from file before processing the next batch
        6. If in continuous mode, sleeps for 30 seconds when there are no new blocks to process
        """
        try:
            self.load_catalog_from_file()
            
            while not terminate_event.is_set():
                latest_block_height = self.block_stream_manager.get_latest_block_height()
                if latest_block_height == 0:
                    logger.warning("No blocks found in the database")
                    if self.continuous:
                        logger.info("Sleeping for 30 seconds before checking again...")
                        time.sleep(30)
                        continue
                    else:
                        return
                
                start_height = self.last_processed_height + 1 if self.last_processed_height > 0 else 1
                if start_height > latest_block_height:
                    if self.continuous:
                        logger.info(f"No new blocks to process. Last processed: {self.last_processed_height}, Latest: {latest_block_height}")
                        logger.info("Sleeping for 30 seconds before checking again...")
                        time.sleep(30)
                        continue
                    else:
                        logger.info(f"No new blocks to process. Last processed: {self.last_processed_height}, Latest: {latest_block_height}")
                        break
                
                logger.info(f"Starting event catalog extraction from block {start_height} to {latest_block_height}")
                
                current_height = start_height
                while current_height <= latest_block_height and not terminate_event.is_set():
                    try:
                        end_height = min(current_height + self.batch_size - 1, latest_block_height)
                        self.process_batch(current_height, end_height)
                        
                        try:
                            self.write_catalog_to_file()
                        except Exception as e:
                            logger.error(f"Error writing catalog to file: {e}", error=e, trb=traceback.format_exc())
                            self._write_simplified_catalog()
                        
                        self.load_catalog_from_file()
                        
                        current_height = end_height + 1
                        self.last_processed_height = end_height
                        
                    except Exception as e:
                        logger.error(f"Error processing batch {current_height} to {end_height}: {e}", error=e, trb=traceback.format_exc())
                        # Skip this batch and continue with the next one
                        current_height = end_height + 1
                        self.last_processed_height = end_height
                        
                    if terminate_event.is_set():
                        logger.info("Termination requested, stopping processing")
                        break
                
                if not self.continuous:
                    break
            
            logger.info(f"Event catalog extraction completed. Processed {self.processed_blocks} blocks.")
            logger.info(f"Found {len(self.event_catalog)} unique module_id/event_id pairs.")
            logger.info(f"Results written to {self.output_file}")
            
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Error during event catalog extraction: {e}", error=e, trb=traceback.format_exc())
            raise
        finally:
            # Close connections
            self.block_stream_manager.close()
            
    def process_batch(self, start_height: int, end_height: int):
        """
        Process a batch of blocks.
        
        Args:
            start_height: Starting block height (inclusive)
            end_height: Ending block height (inclusive)
        """
        try:
            blocks = self.block_stream_manager.get_blocks_by_range(start_height, end_height)
            if not blocks:
                logger.warning(f"No blocks found in range {start_height} to {end_height}")
                return
                
            for block in blocks:
                for event in block.get('events', []):
                    module_id = event.get('module_id', '')
                    event_id = event.get('event_id', '')
                    attributes = event.get('attributes', {})
                    
                    # Skip if module_id or event_id is empty
                    if not module_id or not event_id:
                        continue
                        
                    # Add to catalog if not already present
                    key = f"{module_id}:{event_id}"
                    if key not in self.event_catalog:
                        self.event_catalog[key] = {
                            "attributes": attributes
                        }
                    elif not isinstance(self.event_catalog[key]["attributes"], dict) and isinstance(attributes, dict):
                        # If current attributes is not a dict but new one is, prefer the dict
                        self.event_catalog[key]["attributes"] = attributes
            
            self.processed_blocks += len(blocks)
            logger.info(f"Processed blocks {start_height} to {end_height} ({len(blocks)} blocks)")
            
        except Exception as e:
            logger.error(f"Error processing batch {start_height} to {end_height}: {e}", error=e, trb=traceback.format_exc())
            raise
            
    def write_catalog_to_file(self):
        """
        Write the current catalog to file.
        
        This method:
        1. Creates a temporary file
        2. Writes each module_id/event_id pair and its attributes
        3. Writes metadata (last processed height, processed blocks, unique events)
        4. Replaces the original file with the temporary file
        """
        try:
            temp_file = f"{self.output_file}.tmp"
            
            with open(temp_file, 'w') as f:
                f.write(f"__LAST_PROCESSED_HEIGHT__: {self.last_processed_height}\n\n")
                
                for key, data in sorted(self.event_catalog.items()):
                    f.write(f"{key}\n")
                    attributes = data.get("attributes", {})
                    
                    if isinstance(attributes, dict):
                        for attr_key, attr_value in attributes.items():
                            if isinstance(attr_value, dict) or isinstance(attr_value, list):
                                attr_str = json.dumps(attr_value)
                            else:
                                attr_str = str(attr_value)
                                
                            f.write(f"  {attr_key}: {attr_str}\n")
                    elif isinstance(attributes, (list, tuple)):
                        for i, attr_value in enumerate(attributes):
                            attr_str = str(attr_value)
                            if isinstance(attr_value, dict) or isinstance(attr_value, list):
                                attr_str = json.dumps(attr_value)
                            f.write(f"  {i}: {attr_str}\n")
                    else:
                        f.write(f"  value: {str(attributes)}\n")
                        
                    f.write("\n")
                    
                f.write("__METADATA__\n")
                f.write(f"  last_processed_height: {self.last_processed_height}\n")
                f.write(f"  processed_blocks: {self.processed_blocks}\n")
                f.write(f"  unique_events: {len(self.event_catalog)}\n")
                f.write("\n")
            
            if os.path.exists(self.output_file):
                os.remove(self.output_file)
            os.rename(temp_file, self.output_file)
            
            logger.info(f"Wrote catalog to {self.output_file} ({len(self.event_catalog)} unique events)")
            
        except Exception as e:
            logger.error(f"Error writing catalog to file: {e}", error=e, trb=traceback.format_exc())
            # Don't raise the exception, let the caller handle it
            
    def load_catalog_from_file(self):
        """
        Load the catalog from file if it exists.
        
        This method:
        1. Checks if the file exists
        2. Parses the file to extract module_id/event_id pairs and their attributes
        3. Updates the event_catalog dictionary
        4. Extracts metadata (last processed height, processed blocks)
        """
        if not os.path.exists(self.output_file):
            logger.info(f"No existing catalog file found at {self.output_file}")
            return
            
        try:
            self.event_catalog = {}
            
            with open(self.output_file, 'r') as f:
                lines = f.readlines()
                
            current_key = None
            current_attributes = {}
            
            for line in lines:
                line = line.rstrip()
                
                if line.startswith("__LAST_PROCESSED_HEIGHT__:"):
                    try:
                        self.last_processed_height = int(line.split(':', 1)[1].strip())
                        logger.info(f"Found last processed height: {self.last_processed_height}")
                    except:
                        logger.warning(f"Failed to parse last processed height from: {line}")
                    continue
                
                if not line:
                    if current_key and current_key != "__METADATA__":
                        self.event_catalog[current_key] = {
                            "attributes": current_attributes
                        }
                    current_key = None
                    current_attributes = {}
                    continue
                
                if not line.startswith(' '):
                    if current_key and current_key != "__METADATA__":
                        self.event_catalog[current_key] = {
                            "attributes": current_attributes
                        }
                    
                    current_key = line
                    current_attributes = {}
                    
                    if current_key == "__METADATA__":
                        continue
                        
                elif line.startswith(' ') and current_key:
                    if current_key == "__METADATA__":
                        if line.strip().startswith("last_processed_height:"):
                            try:
                                self.last_processed_height = int(line.split(':', 1)[1].strip())
                            except:
                                pass
                        elif line.strip().startswith("processed_blocks:"):
                            try:
                                self.processed_blocks = int(line.split(':', 1)[1].strip())
                            except:
                                pass
                    else:
                        parts = line.strip().split(':', 1)
                        if len(parts) == 2:
                            attr_key = parts[0]
                            attr_value = parts[1].strip()
                            
                            if (attr_value.startswith('{') and attr_value.endswith('}')) or \
                               (attr_value.startswith('[') and attr_value.endswith(']')):
                                try:
                                    attr_value = json.loads(attr_value)
                                except:
                                    pass
                                    
                            current_attributes[attr_key] = attr_value
            
            if current_key and current_key != "__METADATA__":
                self.event_catalog[current_key] = {
                    "attributes": current_attributes
                }
                
            logger.info(f"Loaded catalog from {self.output_file} ({len(self.event_catalog)} unique events)")
            logger.info(f"Last processed height: {self.last_processed_height}")
            
        except Exception as e:
            logger.error(f"Error loading catalog from file: {e}", error=e, trb=traceback.format_exc())
            self.event_catalog = {}
            self.last_processed_height = 0
            self.processed_blocks = 0
            
    def _write_simplified_catalog(self):
        """
        Write a simplified version of the catalog to file as a fallback.
        This method is called when the normal write_catalog_to_file method fails.
        It writes only the module_id:event_id pairs without attributes.
        """
        try:
            temp_file = f"{self.output_file}.tmp"
            
            with open(temp_file, 'w') as f:
                f.write(f"__LAST_PROCESSED_HEIGHT__: {self.last_processed_height}\n\n")
                for key in sorted(self.event_catalog.keys()):
                    f.write(f"{key}\n\n")
                    
                f.write("__METADATA__\n")
                f.write(f"  last_processed_height: {self.last_processed_height}\n")
                f.write(f"  processed_blocks: {self.processed_blocks}\n")
                f.write(f"  unique_events: {len(self.event_catalog)}\n")
                f.write("\n")
            
            if os.path.exists(self.output_file):
                os.remove(self.output_file)
            os.rename(temp_file, self.output_file)
            
            logger.info(f"Wrote simplified catalog to {self.output_file} ({len(self.event_catalog)} unique events)")
            
        except Exception as e:
            logger.error(f"Error writing simplified catalog to file: {e}", error=e, trb=traceback.format_exc())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Event Catalog Extractor')
    parser.add_argument('--batch-size', type=int, default=100, help='Number of blocks to process in a batch')
    parser.add_argument('--output-file', type=str, help='Output file path (default: event_catalog_<network>.txt)')
    parser.add_argument(
        '--network',
        type=str,
        required=True,
        choices=networks,
        help='Network to extract events from (polkadot, torus, or bittensor)'
    )
    parser.add_argument(
        '--connection-string',
        type=str,
        help='Custom ClickHouse connection string in comma-separated format (host=value,port=value,user=value,password=value,database=value). If not provided, will use the default connection for the network.'
    )
    parser.add_argument(
        '--no-continuous',
        action='store_true',
        help='Disable continuous mode (by default, the script runs continuously, sleeping for 30 seconds when there are no new blocks to process)'
    )
    args = parser.parse_args()

    service_name = f'substrate-{args.network}-event-catalog'
    setup_logger(service_name)

    if args.connection_string:
        try:
            connection_params = {}
            for param in args.connection_string.split(','):
                if '=' in param:
                    key, value = param.strip().split('=', 1)
                    if key == 'port':
                        try:
                            value = int(value)
                        except ValueError:
                            logger.warning(f"Port value '{value}' is not an integer, using as string")
                    connection_params[key] = value
            logger.info(f"Using custom connection string")
            
            required_params = ['host', 'port', 'user', 'password', 'database']
            missing_params = [param for param in required_params if param not in connection_params]
            if missing_params:
                logger.error(f"Missing required parameters in connection string: {', '.join(missing_params)}")
                logger.error("Example of valid format: host=localhost,port=8123,user=default,password=password,database=default")
                exit(1)
                
        except Exception as e:
            logger.error(f"Error parsing connection string: {e}")
            logger.error("Example of valid format: host=localhost,port=8123,user=default,password=password,database=default")
            exit(1)
    else:
        connection_params = get_clickhouse_connection_string(args.network)
    
    create_clickhouse_database(connection_params)
    
    extractor = EventCatalogExtractor(
        connection_params,
        args.network,
        args.batch_size,
        args.output_file,
        service_name,
        not args.no_continuous
    )
    
    try:
        extractor.run()
    except Exception as e:
        logger.error(f"Fatal error", error=e, trb=traceback.format_exc())