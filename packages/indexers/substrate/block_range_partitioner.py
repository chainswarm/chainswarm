from loguru import logger

from packages.indexers.substrate import Network


class BlockRangePartitioner:
    def __init__(self, block_time_seconds: int = 6, num_partitions: int = 32, days: int = 30):
        self.num_partitions = num_partitions
        self.block_time_seconds = block_time_seconds

        blocks_per_day = 24 * 60 * 60 // block_time_seconds
        self.range_size = blocks_per_day * days  # blocks per year

        # Modified to start from block 1
        self.partition_ranges = {
            0: (1, self.range_size),  # First partition starts from 1
            **{
                i: (1 + i * self.range_size, (i + 1) * self.range_size)
                for i in range(1, num_partitions)
            }
        }
        logger.info("Initialized partitioner",
                    partition_ranges=self.partition_ranges,
                    blocks_per_year=self.range_size)

    def __call__(self, block_height):
        # Special case for genesis block
        if block_height == 0:
            return 0  # Genesis block is in partition 0
        
        # Adjust block_height to account for starting from 1
        adjusted_height = block_height - 1
        partition = adjusted_height // self.range_size
        if partition >= self.num_partitions:
            partition = self.num_partitions - 1
        return int(partition)

    def get_partition_range(self, partition):
        if partition == 0:
            start = 1  # First partition starts from 1
        else:
            start = 1 + (partition * self.range_size)
        
        end = ((partition + 1) * self.range_size)
        
        if partition == self.num_partitions - 1:
            end = float('inf')
        
        return (start, end)

    def align_height_to_partition(self, height: int, round_up: bool = False) -> int:
        """Align height to partition boundary"""
        # Special case for genesis block
        if height == 0:
            return 0 if not round_up else 1
        
        # Adjust height to account for starting from 1
        adjusted_height = height - 1
        partition = adjusted_height // self.range_size
        
        if round_up:
            return 1 + (partition + 1) * self.range_size
        return 1 + partition * self.range_size

    def get_partition_for_range(self, start_height: int, end_height: int = None) -> list:
        """Get list of partitions that cover the given height range"""
        start_partition = self.__call__(start_height)
        if end_height is None:
            return list(range(start_partition, self.num_partitions))
        end_partition = self.__call__(end_height)
        return list(range(start_partition, end_partition + 1))


def get_partitioner(network: str):
    if network == Network.POLKADOT.value:
        return BlockRangePartitioner(block_time_seconds=6, num_partitions=256)
    if network == Network.TORUS.value or network == Network.TORUS_TESTNET.value:
        return BlockRangePartitioner(block_time_seconds=8, num_partitions=256)
    if network == Network.BITTENSOR.value:
        return BlockRangePartitioner(block_time_seconds=12, num_partitions=256)
