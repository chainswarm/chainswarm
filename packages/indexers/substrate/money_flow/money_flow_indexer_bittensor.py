import traceback
from loguru import logger
from neo4j import Driver

from packages.indexers.base.metrics import IndexerMetrics
from packages.indexers.substrate.money_flow.money_flow_indexer import BaseMoneyFlowIndexer
from packages.indexers.substrate.assets.asset_manager import AssetManager


class BittensorMoneyFlowIndexer(BaseMoneyFlowIndexer):
    """
    Bittensor-specific implementation of the MoneyFlowIndexer.
    Handles Bittensor-specific events like NeuronRegistered and NetworkAdded
    to enhance address labeling in the graph database.
    """
    
    def __init__(self, graph_database: Driver, network: str, indexer_metrics: IndexerMetrics, asset_manager: AssetManager):
        """
        Initialize the BittensorMoneyFlowIndexer.
        
        Args:
            graph_database: Neo4j driver instance
            network: Network identifier (e.g., 'bittensor', 'bittensor_testnet')
            indexer_metrics: IndexerMetrics instance for recording metrics (required)
            asset_manager: AssetManager instance for managing assets
        """
        super().__init__(graph_database, network, indexer_metrics, asset_manager)
        logger.info(f"Initialized Bittensor money flow indexer for network: {network}")
    
    def _process_network_specific_events(self, transaction, timestamp, events_by_type):
        """
        Process Bittensor-specific events.
        
        Args:
            transaction: Neo4j transaction
            timestamp: Block timestamp
            events_by_type: Dictionary of events grouped by type
        """
        # Process Bittensor-specific events
        self._process_neuron_registered_events(transaction, timestamp, events_by_type.get('SubtensorModule.NeuronRegistered', []))
        self._process_network_added_events(transaction, timestamp, events_by_type.get('SubtensorModule.NetworkAdded', []))
    
    def _process_neuron_registered_events(self, transaction, timestamp, events):
        """
        Process SubtensorModule.NeuronRegistered events.
        
        These events occur when a neuron is registered on a subnet and contain:
        - Network ID (position 0) - The subnet ID
        - Neuron ID (position 1) - The neuron's ID within the subnet
        - Owner address (position 2) - The address that owns the neuron
        
        Args:
            transaction: Neo4j transaction
            timestamp: Block timestamp
            events: List of NeuronRegistered events
        """
        for event in events:
            network_id = event['attributes'][0]
            neuron_id = event['attributes'][1]
            owner_address = event['attributes'][2]
            
            logger.debug(
                "Processing NeuronRegistered event",
                extra={
                    "network_id": network_id,
                    "neuron_id": neuron_id,
                    "owner_address": owner_address
                }
            )
            
            # Create or update the owner address node with neuron owner label
            query = """
            MERGE (addr:Address { address: $owner_address })
            ON CREATE SET
                addr.labels = ['neuron_owner', 'neuron_owner_sn' + $network_id],
                addr.first_seen_timestamp = $timestamp,
                addr.subnets = [$network_id],
                addr.neurons = [{ network_id: $network_id, neuron_id: $neuron_id }]
            ON MATCH SET
                addr.labels = CASE
                    WHEN NOT 'neuron_owner' IN coalesce(addr.labels, [])
                    THEN coalesce(addr.labels, []) + ['neuron_owner']
                    ELSE addr.labels
                END,
                addr.labels = CASE
                    WHEN NOT 'neuron_owner_sn' + $network_id IN coalesce(addr.labels, [])
                    THEN coalesce(addr.labels, []) + ['neuron_owner_sn' + $network_id]
                    ELSE addr.labels
                END,
                addr.subnets = CASE
                    WHEN NOT $network_id IN coalesce(addr.subnets, [])
                    THEN coalesce(addr.subnets, []) + [$network_id]
                    ELSE addr.subnets
                END,
                addr.neurons = CASE
                    WHEN NOT { network_id: $network_id, neuron_id: $neuron_id } IN coalesce(addr.neurons, [])
                    THEN coalesce(addr.neurons, []) + [{ network_id: $network_id, neuron_id: $neuron_id }]
                    ELSE addr.neurons
                END,
                addr.last_updated_timestamp = $timestamp
            """
            transaction.run(query, {
                'owner_address': owner_address,
                'network_id': str(network_id),
                'neuron_id': str(neuron_id),
                'timestamp': timestamp
            })
            
            # Create or update the neuron node
            query = """
            MERGE (neuron:Neuron { 
                network_id: $network_id, 
                neuron_id: $neuron_id 
            })
            SET neuron.owner_address = $owner_address,
                neuron.last_updated_timestamp = $timestamp
            """
            transaction.run(query, {
                'network_id': str(network_id),
                'neuron_id': str(neuron_id),
                'owner_address': owner_address,
                'timestamp': timestamp
            })
            
            # Create relationship between owner and neuron
            query = """
            MATCH (owner:Address { address: $owner_address })
            MATCH (neuron:Neuron { 
                network_id: $network_id, 
                neuron_id: $neuron_id 
            })
            MERGE (owner)-[r:OWNS]->(neuron)
            SET r.last_updated_timestamp = $timestamp
            """
            transaction.run(query, {
                'owner_address': owner_address,
                'network_id': str(network_id),
                'neuron_id': str(neuron_id),
                'timestamp': timestamp
            })
    
    def _process_network_added_events(self, transaction, timestamp, events):
        """
        Process SubtensorModule.NetworkAdded events.
        
        These events occur when a new subnet is created and contain:
        - Network ID (position 0) - The subnet ID
        - Network ID again (position 1) - Duplicate of the first parameter
        
        Args:
            transaction: Neo4j transaction
            timestamp: Block timestamp
            events: List of NetworkAdded events
        """
        for event in events:
            network_id = event['attributes'][0]
            
            logger.debug(
                "Processing NetworkAdded event",
                extra={"network_id": network_id}
            )
            
            # Create or update the subnet node
            query = """
            MERGE (subnet:Subnet { network_id: $network_id })
            ON CREATE SET
                subnet.created_timestamp = $timestamp,
                subnet.label = 'sn' + $network_id
            SET subnet.last_updated_timestamp = $timestamp
            """
            transaction.run(query, {
                'network_id': str(network_id),
                'timestamp': timestamp
            })
            
            # Try to identify the subnet creator from the extrinsic
            # Note: This requires access to the extrinsic data, which might not be available in the event
            if 'extrinsic' in event and 'signer' in event['extrinsic']:
                creator_address = event['extrinsic']['signer']
                
                logger.debug(
                    "Found subnet creator",
                    extra={
                        "creator_address": creator_address,
                        "subnet_id": network_id
                    }
                )
                
                # Update the creator address with subnet creator label
                query = """
                MERGE (addr:Address { address: $creator_address })
                ON CREATE SET
                    addr.labels = ['subnet_creator', 'subnet_creator_sn' + $network_id],
                    addr.first_seen_timestamp = $timestamp,
                    addr.created_subnets = [$network_id]
                ON MATCH SET
                    addr.labels = CASE
                        WHEN NOT 'subnet_creator' IN coalesce(addr.labels, [])
                        THEN coalesce(addr.labels, []) + ['subnet_creator']
                        ELSE addr.labels
                    END,
                    addr.labels = CASE
                        WHEN NOT 'subnet_creator_sn' + $network_id IN coalesce(addr.labels, [])
                        THEN coalesce(addr.labels, []) + ['subnet_creator_sn' + $network_id]
                        ELSE addr.labels
                    END,
                    addr.created_subnets = CASE
                        WHEN NOT $network_id IN coalesce(addr.created_subnets, [])
                        THEN coalesce(addr.created_subnets, []) + [$network_id]
                        ELSE addr.created_subnets
                    END,
                    addr.last_updated_timestamp = $timestamp
                """
                transaction.run(query, {
                    'creator_address': creator_address,
                    'network_id': str(network_id),
                    'timestamp': timestamp
                })
                
                # Create relationship between creator and subnet
                query = """
                MATCH (creator:Address { address: $creator_address })
                MATCH (subnet:Subnet { network_id: $network_id })
                MERGE (creator)-[r:CREATED]->(subnet)
                SET r.timestamp = $timestamp
                """
                transaction.run(query, {
                    'creator_address': creator_address,
                    'network_id': str(network_id),
                    'timestamp': timestamp
                })
            else:
                logger.debug(
                    "Could not identify subnet creator",
                    extra={"subnet_id": network_id}
                )