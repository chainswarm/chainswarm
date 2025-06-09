from typing import TypedDict, List, Dict, Any


class Transaction(TypedDict):
    extrinsic_id: str
    extrinsic_hash: str
    signer: str
    call_module: str
    call_function: str
    status: str


class Event(TypedDict):
    event_idx: str
    extrinsic_id: str
    module_id: str
    event_id: str
    attributes: Dict[str, Any]


class Block(TypedDict):
    block_height: int
    block_hash: str
    timestamp: int
    transactions: List[Transaction]
    addresses: List[str]
    events: List[Event]