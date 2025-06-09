-- Known Addresses Tables Schema
-- These tables store known blockchain addresses with their labels and metadata

-- Main table for storing known addresses
CREATE TABLE IF NOT EXISTS known_addresses (
    id UUID,                      -- Unique identifier for each address entry
    network String,               -- Blockchain network (e.g., 'bittensor', 'torus', 'polkadot')
    address String,               -- The blockchain address
    label String,                 -- Human-readable label for the address
    source String,                -- Source of the address information (e.g., 'github', 'manual')
    source_type String,           -- Type of source (e.g., 'external', 'internal')
    last_updated DateTime,        -- When this entry was last updated
    _version UInt64,              -- Version for ReplacingMergeTree
    PRIMARY KEY (network, address, source)
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (network, address, source);

-- Table to track when known addresses were last updated for each network
CREATE TABLE IF NOT EXISTS known_addresses_updates (
    network String,               -- Blockchain network identifier
    last_updated DateTime DEFAULT now(),  -- Last time addresses were updated for this network
    PRIMARY KEY (network)
)
ENGINE = MergeTree()
ORDER BY network;