import os
import traceback
from loguru import logger
from neo4j import Driver

from packages.indexers.substrate.money_flow.money_flow_indexer import BaseMoneyFlowIndexer
from packages.indexers.substrate.money_flow import populate_genesis_balances
from packages.indexers.substrate import data


class TorusMoneyFlowIndexer(BaseMoneyFlowIndexer):
    """
    Torus-specific implementation of the MoneyFlowIndexer.
    Handles Torus-specific events like AgentRegistered.
    """
    
    def __init__(self, graph_database: Driver, network: str):
        """
        Initialize the TorusMoneyFlowIndexer.
        
        Args:
            graph_database: Neo4j driver instance
            network: Network identifier (e.g., 'torus', 'torus_testnet')
        """
        super().__init__(graph_database, network)
        
        # Initialize genesis balances for Torus networks
        self._init_genesis_balances()
    
    def _process_network_specific_events(self, transaction, timestamp, events_by_type):
        """
        Process Torus-specific events.
        
        Args:
            transaction: Neo4j transaction
            timestamp: Block timestamp
            events_by_type: Dictionary of events grouped by type
        """
        # Process Torus-specific events
        self._process_agent_registered_events(transaction, timestamp, events_by_type.get('Torus0.AgentRegistered', []))
    
    def _process_agent_registered_events(self, transaction, timestamp, events):
        """
        Process torus0.AgentRegistered events.
        
        Args:
            transaction: Neo4j transaction
            timestamp: Block timestamp
            events: List of AgentRegistered events
        """
        for event in events:
            agent = event['attributes']
            query = """
            MERGE (agent:Address { address: $agent })
            SET agent:Agent,
                agent.labels = coalesce(agent.labels, []) + ['agent']
            """
            transaction.run(query, {
                'agent': agent,
                'asset': self.asset
            })
    
    def _init_genesis_balances(self):
        """Initialize genesis balances for Torus networks if they don't exist yet"""
        try:
            # Check if we already have genesis addresses for this asset
            with self.graph_database.session() as session:
                result = session.run("""
                    MATCH (addr:Address:Genesis)
                    RETURN COUNT(addr) AS genesis_count
                """)
                record = result.single()
                if record and record["genesis_count"] > 0:
                    logger.info(f"Genesis address records already exist for asset {self.asset}, skipping genesis balance initialization")
                    return
                    
            # Check if genesis balances file exists
            file_path = os.path.join(os.path.dirname(os.path.abspath(data.__file__)), "torus-genesis-balances.json")
            if not os.path.exists(file_path):
                logger.warning(f"No genesis balances file found for Torus network at {file_path}, skipping genesis balance initialization")
                return
            
            # Load and populate genesis balances
            logger.info(f"Loading genesis balances from {file_path}")
            populate_genesis_balances.run(file_path, self.network)
            logger.success(f"Genesis balances initialized for Torus network: {self.network}")
            
        except Exception as e:
            logger.error(f"Error initializing genesis balances for Torus network: {e}", error=e, trb=traceback.format_exc())