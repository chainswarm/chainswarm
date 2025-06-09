from abc import ABC, abstractmethod
from typing import List, Dict, Any


class Node(ABC):
    def __init__(self):
       pass

    @abstractmethod
    def get_current_block_height(self) -> int:
        """Get current blockchain height"""
        ...

    @abstractmethod
    def get_block_by_height(self, block_height: int) -> Dict[str, Any] | None:
        """Get block data at specified height"""
        ...
