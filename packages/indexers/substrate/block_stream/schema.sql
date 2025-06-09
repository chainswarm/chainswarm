-- Block Stream Table Schema
-- This table stores comprehensive blockchain data including blocks, transactions, addresses, and events
-- Uses nested structures for efficient storage and querying of related data

CREATE TABLE IF NOT EXISTS block_stream (
    -- Block information
    block_height UInt64,
    block_hash String,
    block_timestamp UInt64,

    -- Nested structure for transactions (extrinsics)
    transactions Nested (
        extrinsic_id String,      -- Format: {block_height}-{index}
        extrinsic_hash String,    -- Transaction hash
        signer String,            -- Address of the transaction signer
        call_module String,       -- Module name of the call
        call_function String,     -- Function name of the call
        status String             -- Transaction status: Success/Failed
    ),

    -- Array of all addresses involved in this block
    addresses Array(String),

    -- Nested structure for events
    events Nested (
        event_idx String,         -- Format: {block_height}-{index}
        extrinsic_id String,      -- Reference to the extrinsic that triggered this event
        module_id String,         -- Module that emitted the event
        event_id String,          -- Event name
        attributes String         -- JSON-serialized event attributes
    ),

    -- Version for ReplacingMergeTree
    _version UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY block_height
PARTITION BY intDiv(block_height, {partition_size});