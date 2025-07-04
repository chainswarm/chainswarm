-- Assets Dictionary Table Schema
-- This table stores information about all assets (native and tokens) across different networks

CREATE TABLE IF NOT EXISTS assets (
    network String,
    asset_symbol String,
    asset_contract String,
    asset_verified String DEFAULT 'unknown',
    asset_name String DEFAULT '',
    asset_type String DEFAULT 'token',
    decimals UInt8 DEFAULT 0,
    first_seen_block UInt32,
    first_seen_timestamp DateTime,
    last_updated DateTime DEFAULT now(),
    updated_by String DEFAULT 'system',
    notes String DEFAULT '',
    PRIMARY KEY (network, asset_contract)
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (network, asset_contract);

-- Create index for efficient lookups by symbol
CREATE INDEX IF NOT EXISTS idx_assets_symbol ON assets (network, asset_symbol) TYPE minmax;

-- Create index for filtering by asset type
CREATE INDEX IF NOT EXISTS idx_assets_type ON assets (network, asset_type) TYPE set(100);

-- Create index for filtering by verification status
CREATE INDEX IF NOT EXISTS idx_assets_verified ON assets (network, asset_verified) TYPE set(10);


-- Insert native assets for supported networks
-- These are the primary native assets for each chain
INSERT INTO assets (network, asset_symbol, asset_contract, asset_verified, asset_name, asset_type, decimals, first_seen_block, first_seen_timestamp)
VALUES
    -- Torus native asset
    ('torus', 'TOR', 'native', 'verified', 'Torus', 'native', 18, 0, now()),
    
    -- Bittensor native asset
    ('bittensor', 'TAO', 'native', 'verified', 'Bittensor', 'native', 18, 0, now()),
    
    -- Polkadot native asset
    ('polkadot', 'DOT', 'native', 'verified', 'Polkadot', 'native', 10, 0, now());
