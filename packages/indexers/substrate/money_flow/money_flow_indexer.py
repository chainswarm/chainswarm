import traceback
import functools
import time
from typing import Optional, List, Dict

from loguru import logger
from neo4j import Driver
from packages.indexers.base import terminate_event
from packages.indexers.base.decimal_utils import convert_to_decimal_units
from packages.indexers.substrate import get_network_asset


def infinite_retry_with_backoff(method):
    """
    Decorator for methods to implement infinite retry with exponential backoff.

    This decorator will:
    1. Retry the method indefinitely until success or explicit termination
    2. Implement exponential backoff to avoid overwhelming the database
    3. Check for termination events to allow graceful shutdown
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        retry_count = 0
        backoff_time = 1  # Start with 1 second

        while True:  # Infinite loop
            try:
                # Attempt the operation
                return method(self, *args, **kwargs)

            except Exception as e:
                retry_count += 1
                error_message = str(e)

                # Log less frequently for long-running retries
                if retry_count == 1 or retry_count % 10 == 0:
                    logger.warning(f"Retry {retry_count} for {method.__name__}: {e}")

                # Check for termination before continuing
                if hasattr(self, 'terminate_event') and self.terminate_event.is_set():
                    logger.info(f"Termination requested during {method.__name__} retry")
                    raise RuntimeError(f"Operation {method.__name__} terminated during retry")

                # Check global termination event
                if terminate_event.is_set():
                    logger.info(f"Termination requested during {method.__name__} retry")
                    raise RuntimeError(f"Operation {method.__name__} terminated during retry")

                # Exponential backoff with maximum of 60 seconds
                backoff_time = min(backoff_time * 1.5, 60)
                time.sleep(backoff_time)

                # Continue the loop (infinite retry)

    return wrapper


class BaseMoneyFlowIndexer:
    """
    Base class for money flow indexers that provides common functionality for all networks.
    Network-specific implementations should inherit from this class and override the
    _process_network_specific_events method.
    """

    def __init__(self, graph_database: Driver, network: str):
        """
        Initialize the BaseMoneyFlowIndexer.

        Args:
            graph_database: Neo4j driver instance
            network: Network identifier (e.g., 'torus', 'bittensor', 'polkadot')
        """
        self.graph_database = graph_database
        self.terminate_event = terminate_event  # Store reference to global termination event
        self.network = network
        self.asset = get_network_asset(network)  # Get the asset symbol for this network

    def index_blocks(self, blocks):
        with self.graph_database.session() as session:
            for block in blocks:
                # Check for termination before processing each block
                if self.terminate_event.is_set():
                    logger.info(f"Termination requested, skipping block {block.get('block_height')}")
                    break
                self.index_block(session, block)

    def create_indexes(self):
        with self.graph_database.session() as session:
            result = session.run("SHOW INDEX INFO;")
            existing_indexes = set()
            for row in result:
                entity = row.get("label") or row.get("edge_type")
                prop = row.get("property")
                if entity and prop:
                    existing_indexes.add(f"{entity}:{prop}")
                elif entity:
                    existing_indexes.add(f"{entity}")

            indexes = [
                ("Address", "address"),
                ("Address", "volume_in"),
                ("Address", "volume_out"),
                ("Address", "transfer_count"),
                ("Address", "neighbor_count"),
                ("Address", "first_transfer_block_height"),
                ("Address", "first_transfer_timestamp"),
                ("Address", "last_transfer_block_height"),
                ("Address", "last_transfer_timestamp"),
                # New calculated properties
                ("Address", "volume_differential"),
                ("Address", "log_transfer_count"),
                ("Address", "outgoing_tx_avg_ratio"),
                ("Address", "incoming_tx_avg_ratio"),
                ("Address", "avg_outgoing_tx_frequency"),
                ("Address", "avg_incoming_tx_frequency"),
                ("Address", "unique_senders"),
                ("Address", "unique_receivers"),
                ("Agent", "address"),
                ("Agent", "labels"),
            ]

            edge_indexes = [
                ("TO", "id"),
                ("TO", "asset"),
                ("TO", "volume"),
                ("TO", "min_amount"),
                ("TO", "max_amount"),
                ("TO", "transfer_count"),
                ("TO", "last_transfer_block_height"),
                ("TO", "last_transfer_timestamp"),
                ("TO", "first_transfer_block_height"),
                ("TO", "first_transfer_timestamp"),
            ]

            for label, prop in indexes:
                base_label = label.lstrip(":")
                index_key = f"{base_label}:{prop}"
                if index_key not in existing_indexes:
                    session.run(f"CREATE INDEX ON :{base_label}({prop});")

            for edge_type, prop in edge_indexes:
                base_edge = edge_type.lstrip(":")
                index_key = f"{base_edge}:{prop}"
                if index_key not in existing_indexes:
                    session.run(f"CREATE EDGE INDEX ON :{base_edge}({prop});")

            result = session.run("CALL vector_search.show_index_info() YIELD * RETURN *;")
            existing_vector_indexes = set()
            for row in result:
                entity = row.get("label")
                prop = row.get("property")
                existing_vector_indexes.add(f"{entity}:{prop}")

            financial_index_key = "Address:financial_embedding"
            if financial_index_key not in existing_vector_indexes:
                session.run("""
                            CREATE VECTOR INDEX FinancialEmbeddings
                            ON:Address(financial_embedding)
                            WITH CONFIG {
                            "capacity":1000,
                            "dimension":6,
                            "metric":"cos"
                            };
                            """)
                logger.info("Created Financial vector index")

            temporal_index_key = "Address:temporal_embedding"
            if temporal_index_key not in existing_vector_indexes:
                session.run("""
                            CREATE VECTOR INDEX TemporalEmbeddings
                            ON:Address(temporal_embedding)
                            WITH CONFIG {
                            "capacity":1000,
                            "dimension":4,
                            "metric":"cos"
                            };
                            """)
                logger.info("Created Temporal vector index")

            network_index_key = "Address:network_embedding"
            if network_index_key not in existing_vector_indexes:
                session.run("""
                            CREATE VECTOR INDEX NetworkEmbeddings
                            ON:Address(network_embedding)
                            WITH CONFIG {
                            "capacity":1000,
                            "dimension":4,
                            "metric":"cos"
                            };
                            """)
                logger.info("Created Network vector index")

            joint_index_key = "Address:joint_embedding"
            if joint_index_key not in existing_vector_indexes:
                session.run("""
                            CREATE VECTOR INDEX JointEmbeddings
                            ON:Address(joint_embedding)
                            WITH CONFIG {
                            "capacity":1000,
                            "dimension":14,
                            "metric":"cos"
                            };
                            """)
                logger.info("Created Joint vector index")

    def update_global_state(self, end_height):
        """
        Index an empty block with infinite retry.

        Args:
            session: Neo4j session
            end_height: Block height to index

        Raises:
            Exception: If there's an error during indexing
        """
        try:
            with self.graph_database.session() as session:
                with session.begin_transaction() as transaction:
                    transaction.run("""
                                    MERGE (g:GlobalState {name: 'last_block_height'})
                                    SET
                                    g.block_height = $end_height
                                    """, {
                                        'end_height': end_height
                                    })
        except Exception as e:
            logger.error(f"Error indexing empty block", error=e, trb=traceback.format_exc())
            raise e

    @infinite_retry_with_backoff
    def index_block(self, session, block):
        """
        Index money flow events from a block's events with infinite retry.

        Args:
            session: Neo4j session
            block: Block data containing events to process

        Raises:
            Exception: If there's an error during indexing
        """
        try:
            events_by_type = self._group_events(block.get('events', []))
            block_height = block.get('block_height')
            timestamp = block.get('timestamp')

            with session.begin_transaction() as transaction:
                result = transaction.run("""
                MATCH (g:GlobalState { name: "last_block_height" })
                RETURN g.block_height AS last_block_height
                """)
                last_block_height = result.single()
                """
                if last_block_height is None and block_height > 1:
                    raise ValueError(f"Cannot index block {block_height} without indexing block 0 first")
                elif last_block_height is not None:
                    last_block_height = last_block_height['last_block_height']
                    if last_block_height + 1 != block_height:
                        raise ValueError(f"Cannot index block {block_height} before block {last_block_height}")
                """
            with session.begin_transaction() as transaction:
                transaction.run("""
                                MERGE (g:GlobalState { name: "last_block_height" })
                                SET
                                  g.block_height = $block_height
                                """, {
                    'block_height': block_height
                })

                # Process common events for all networks
                self._process_endowed_events(transaction, timestamp, events_by_type.get('Balances.Endowed', []))
                self._process_transfer_events(transaction, timestamp, events_by_type.get('Balances.Transfer', []))

                # Process network-specific events
                self._process_network_specific_events(transaction, timestamp, events_by_type)

        except Exception as e:
            logger.error(f"Error indexing transaction", error=e, trb=traceback.format_exc())
            raise e

    def extract_addresses_from_blocks(self, blocks: List[Dict]) -> List[str]:
        """Extract unique addresses from blocks without processing them"""
        address_set = set()

        for block in blocks:
            events = block.get('events', [])
            events_by_type = self._group_events(events)

            for event in events_by_type.get('Balances.Transfer', []):
                attrs = event['attributes']
                address_set.add(attrs['from'])
                address_set.add(attrs['to'])

            for event in events_by_type.get('Balances.Endowed', []):
                address_set.add(event['attributes']['account'])

        addresses = []
        for address_set in address_set:
            if address_set not in addresses:
                addresses.append(address_set)

        return addresses

    @infinite_retry_with_backoff
    def update_calculated_properties(self, addresses: Optional[List[str]] = None):
        """Update calculated properties for addresses before embedding generation"""
        try:
            base_query = """
            MATCH (a:Address)
            {address_filter}
            
            // Calculate outgoing transaction metrics (asset-aware)
            OPTIONAL MATCH (a)-[r:TO]->(target)
            WHERE r.asset = $asset
            WITH a,
             collect(r) as outgoing_txs,
             avg(r.volume) as avg_out_volume,
             sum(r.transfer_count) as total_out_transfers,
             count(DISTINCT target) as unique_receivers_count
             
            // Calculate incoming transaction metrics (asset-aware)
            OPTIONAL MATCH (source)-[in_r:TO]->(a)
            WHERE in_r.asset = $asset
            WITH a, outgoing_txs, avg_out_volume, total_out_transfers, unique_receivers_count,
             collect(in_r) as incoming_txs,
             avg(in_r.volume) as avg_in_volume,
             sum(in_r.transfer_count) as total_in_transfers,
             count(DISTINCT source) as unique_senders_count
            
            // Set all calculated properties
            SET a.volume_differential = coalesce(a.volume_in, 0) - coalesce(a.volume_out, 0),
            a.log_transfer_count = log(coalesce(a.transfer_count, 0) + 1),
            a.outgoing_tx_avg_ratio = CASE
                WHEN coalesce(avg_out_volume, 0) = 0 THEN 0
                ELSE coalesce(total_out_transfers, 0) / (coalesce(avg_out_volume, 0) + 0.001)
            END,
            a.incoming_tx_avg_ratio = CASE
                WHEN coalesce(avg_in_volume, 0) = 0 THEN 0
                ELSE coalesce(total_in_transfers, 0) / (coalesce(avg_in_volume, 0) + 0.001)
            END,
            a.avg_outgoing_tx_frequency = CASE
                WHEN (coalesce(a.last_transfer_timestamp, 0) - coalesce(a.first_transfer_timestamp, 0)) = 0 THEN 0
                ELSE total_out_transfers::float / ((coalesce(a.last_transfer_timestamp, 0) - coalesce(a.first_transfer_timestamp, 0)) / 86400000.0)
            END,
            a.avg_incoming_tx_frequency = CASE
                WHEN (coalesce(a.last_transfer_timestamp, 0) - coalesce(a.first_transfer_timestamp, 0)) = 0 THEN 0
                ELSE total_in_transfers::float / ((coalesce(a.last_transfer_timestamp, 0) - coalesce(a.first_transfer_timestamp, 0)) / 86400000.0)
            END,
            a.unique_senders = unique_senders_count,
            a.unique_receivers = unique_receivers_count
            """

            if addresses is not None:
                query = base_query.replace("{address_filter}", "AND a.address IN $addresses")
                params = {"addresses": addresses, "asset": self.asset}
                with self.graph_database.session() as session:
                    session.run(query, params)
            else:
                query = base_query.replace("{address_filter}", "")
                params = {"asset": self.asset}
                with self.graph_database.session() as session:
                    session.run(query, params)

        except Exception as e:
            logger.error("Failed to update calculated properties", error=e)
            raise

    @infinite_retry_with_backoff
    def update_embeddings(self, addresses: Optional[List[str]] = None):
        """Update joint embeddings using pre-calculated properties"""
        try:
            # First update calculated properties
            self.update_calculated_properties(addresses)

            # Then create embeddings from the properties
            base_query = """
            MATCH (a:Address)
            {address_filter}

            SET a.financial_embedding = [
                // Financial features (6 dimensions) - now just reading properties
                coalesce(a.volume_in, 0) / 1e8,                    // Normalized incoming volume
                coalesce(a.volume_out, 0) / 1e8,                   // Normalized outgoing volume
                coalesce(a.volume_differential, 0) / 1e8,          // Volume differential
                coalesce(a.log_transfer_count, 0),                 // Log-scaled transaction count
                coalesce(a.outgoing_tx_avg_ratio, 0),              // Transaction count to avg volume ratio (outgoing)
                coalesce(a.incoming_tx_avg_ratio, 0)               // Transaction count to avg volume ratio (incoming)
            ],

            a.temporal_embedding = [
                // Temporal features (4 dimensions) - using existing timestamps
                coalesce(a.last_transfer_timestamp, 0) / 1e12,         // Last activity time (normalized)
                coalesce(a.first_transfer_timestamp, 0) / 1e12,        // First activity time (normalized)
                coalesce(a.avg_outgoing_tx_frequency, 0),              // Average outgoing transaction frequency
                coalesce(a.avg_incoming_tx_frequency, 0)               // Average incoming transaction frequency
            ],

            a.network_embedding = [
                // Network features (4 dimensions) - now just reading properties
                coalesce(a.page_rank, 0),                              // PageRank score
                coalesce(a.community_id, 0),                           // Community membership
                coalesce(a.unique_senders, 0),                         // Number of unique addresses that sent to this address
                coalesce(a.unique_receivers, 0)                        // Number of unique addresses this address sent to
            ],

            a.joint_embedding = a.financial_embedding +
                               a.temporal_embedding +
                               a.network_embedding
            """

            if addresses is not None:
                query = base_query.replace("{address_filter}", "AND a.address IN $addresses")
                params = {"addresses": addresses, "asset": self.asset}
                with self.graph_database.session() as session:
                    session.run(query, params)
            else:
                query = base_query.replace("{address_filter}", "")
                params = {"asset": self.asset}
                with self.graph_database.session() as session:
                    session.run(query, params)

        except Exception as e:
            logger.error("Failed to update embeddings", error=e)
            raise

    @infinite_retry_with_backoff
    def community_detection(self):
        """Run community detection with infinite retry"""
        try:
            query = """
                   MATCH (source:Address)-[r:TO]->(target:Address)
                   WHERE r.asset = $asset
                   WITH collect(DISTINCT source) + collect(DISTINCT target) AS nodes, collect(DISTINCT r) AS relationships
                   CALL leiden_community_detection.get_subgraph(nodes, relationships)
                   YIELD node, community_id, communities
                   SET node.community_id = community_id, node.community_ids = communities
                   WITH DISTINCT community_id
                   WHERE community_id IS NOT NULL
                   MERGE (c:Community { community_id: community_id, asset: $asset });
                """
            with self.graph_database.session() as session:
                with session.begin_transaction() as transaction:
                    transaction.run(query, {"asset": self.asset})
        except Exception as e:
            if e.args[0] == 'leiden_community_detection.get_subgraph: No communities detected.':
                logger.warning("No communities detected")
            else:
                logger.error(f"Error running community detection query", error=e, trb=traceback.format_exc())
                raise e

    @infinite_retry_with_backoff
    def page_rank_with_community(self):
        """Run PageRank with community with infinite retry"""
        try:
            with self.graph_database.session() as session:
                result = session.run(
                    "MATCH (c:Community) WHERE c.asset = $asset RETURN DISTINCT c.community_id",
                    {"asset": self.asset}
                )
                communities = [community_id[0] for community_id in result]

                # Log summary before starting
                logger.info(f"Starting PageRank for {len(communities)} communities (asset: {self.asset})")
                start_time_total = time.time()
                processed_count = 0

                for community in communities:
                    # Check for termination between communities
                    if self.terminate_event.is_set():
                        logger.info(
                            f"Termination requested during PageRank, stopping after {processed_count}/{len(communities)} communities")
                        break

                    # Log at DEBUG level instead of INFO
                    logger.debug(f"Running PageRank for community {community} (asset: {self.asset})")
                    community_start_time = time.time()

                    subgraph_query = f"""
                        MATCH p=(a1:Address {{community_id: {community!r}}})-[r:TO*1..3]->(a2:Address)
                        WHERE ALL(rel IN r WHERE rel.asset = $asset)
                        WITH project(p) AS community_graph
                        CALL pagerank.get(community_graph) YIELD node, rank
                        SET node.page_rank = rank
                        """
                    with session.begin_transaction() as transaction:
                        transaction.run(subgraph_query, {"asset": self.asset})

                    processed_count += 1
                    community_end_time = time.time()
                    logger.debug(
                        f"PageRank for community {community} took {community_end_time - community_start_time:.2f} seconds")

                # Log summary after completion
                end_time_total = time.time()
                total_duration = end_time_total - start_time_total
                avg_duration = total_duration / processed_count if processed_count > 0 else 0
                logger.success(
                    f"Completed PageRank for {processed_count}/{len(communities)} communities in {total_duration:.2f} seconds (avg: {avg_duration:.2f}s per community) (asset: {self.asset})")

        except Exception as e:
            logger.error("Error running PageRank query", error=e, trb=traceback.format_exc())
            raise e

    def _group_events(self, events):
        """Group events by module.event_name for easier processing"""
        grouped = {}
        for event in events:
            key = f"{event['module_id']}.{event['event_id']}"
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(event)
        return grouped

    def _process_endowed_events(self, transaction, timestamp, events):
        """Process Balances.Endowed events"""
        for event in events:
            attrs = event['attributes']
            query = """
            MERGE (addr:Address { address: $account })
            ON CREATE SET
                addr.first_transfer_block_height = $block_height,
                addr.first_transfer_timestamp = $timestamp

            """
            transaction.run(query, {
                'block_height': event['block_height'],
                'timestamp': timestamp,
                'account': attrs['account'],
                'asset': self.asset,
                'amount': float(convert_to_decimal_units(
                    attrs['free_balance'],
                    self.network
                ))
            })

    def _process_transfer_events(self, transaction, timestamp, events):
        """Process Balances.Transfer events with consolidated relationship merge."""
        for event in events:
            attrs = event['attributes']
            amount = float(convert_to_decimal_units(
                attrs['amount'],
                self.network
            ))
            query = """
            MERGE (sender:Address { address: $from })
              ON CREATE SET
                sender.first_transfer_block_height = $block_height,
                sender.first_transfer_timestamp = $timestamp,
                sender.fraud = 0
              SET sender.volume_out = coalesce(sender.volume_out, 0) + $amount,
                  sender.transfer_count = coalesce(sender.transfer_count, 0) + 1,
                  sender.last_transfer_block_height = $block_height,
                  sender.last_transfer_timestamp = $timestamp
            MERGE (receiver:Address { address: $to })
              ON CREATE SET
                receiver.first_transfer_block_height = $block_height,
                receiver.first_transfer_timestamp = $timestamp,
                receiver.fraud = 0
              SET receiver.volume_in = coalesce(receiver.volume_in, 0) + $amount,
                receiver.transfer_count = coalesce(receiver.transfer_count, 0) + 1,
                receiver.last_transfer_block_height = $block_height,
                receiver.last_transfer_timestamp = $timestamp

            MERGE (sender)-[r:TO { id: $to_id, asset: $asset }]->(receiver)
              ON CREATE SET
                  r.volume = $amount,
                  r.min_amount = $amount,
                  r.max_amount = $amount,
                  r.transfer_count = 1,
                  r.last_transfer_block_height = $block_height,
                  r.last_transfer_timestamp = $timestamp,
                  r.first_transfer_block_height = $block_height,
                  r.first_transfer_timestamp = $timestamp,
                  sender.neighbor_count = coalesce(sender.neighbor_count, 0) + 1,
                  receiver.neighbor_count = coalesce(receiver.neighbor_count, 0) + 1
              ON MATCH SET
                  r.volume = r.volume + $amount,
                  r.min_amount = CASE WHEN $amount < r.min_amount THEN $amount ELSE r.min_amount END,
                  r.max_amount = CASE WHEN $amount > r.max_amount THEN $amount ELSE r.max_amount END,
                  r.transfer_count = r.transfer_count + 1,
                  r.last_transfer_block_height = $block_height,
                  r.last_transfer_timestamp = $timestamp,
                  r.first_transfer_block_height = CASE WHEN r.first_transfer_block_height IS NULL THEN $block_height ELSE r.first_transfer_block_height END,
                  r.first_transfer_timestamp = CASE WHEN r.first_transfer_timestamp IS NULL THEN $timestamp ELSE r.first_transfer_timestamp END
            """
            transaction.run(query, {
                'block_height': event['block_height'],
                'timestamp': timestamp,
                'from': attrs['from'],
                'to': attrs['to'],
                'amount': amount,
                'asset': self.asset,
                'to_id': f"from-{attrs['from']}-to-{attrs['to']}-{self.asset}",
            })

    def _process_network_specific_events(self, transaction, timestamp, events_by_type):
        """
        Process network-specific events. To be overridden by subclasses.

        Args:
            transaction: Neo4j transaction
            timestamp: Block timestamp
            events_by_type: Dictionary of events grouped by type
        """
        pass  # Base implementation does nothing


"""
Example queries:

-- Query by calculated properties directly
MATCH (a:Address)
WHERE a.outgoing_tx_frequency > 0.5
AND a.volume_differential < 0
RETURN a.address, a.outgoing_tx_frequency, a.volume_differential
LIMIT 10;

-- Financial pattern matching (using simple embeddings)
CALL vector_search.search("FinancialEmbeddings", 5, [0.8, 0.2, 0.1, 1.2, 0.5, 0.3])

-- Temporal pattern matching (using simple embeddings)
CALL vector_search.search("TemporalEmbeddings", 5, [0.01, 0.005, 0.8, 0.6])

-- Network structure analysis (using simple embeddings)
CALL vector_search.search("NetworkEmbeddings", 5, [0.5, 42, 10, 15])

-- Combined analysis (using simple embeddings)
CALL vector_search.search("JointEmbeddings", 5, [0.8,0.2,0.1,1.2,0.5,0.3,0.01,0.005,0.8,0.6,0.5,42,10,15])
"""