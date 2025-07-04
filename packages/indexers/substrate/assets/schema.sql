-- Assets Dictionary Table Schema
-- This table stores information about all assets (native and tokens) across different networks

CREATE TABLE IF NOT EXISTS assets (
    network String,
    asset_symbol String,  -- Symbol/ticker (e.g., USDT, TAO, TOR)
    asset_contract String,  -- Contract address or 'native' for native assets
    asset_verified String DEFAULT 'unknown',
    asset_name String DEFAULT '',  -- Full display name
    asset_type String DEFAULT 'token',  -- 'native' or 'token'
    decimals UInt8 DEFAULT 0,
    first_seen_block UInt32,
    first_seen_timestamp DateTime,
    last_updated DateTime DEFAULT now(),
    updated_by String DEFAULT 'system',
    notes String DEFAULT '',
    PRIMARY KEY (network, asset_contract),
    CONSTRAINT valid_asset_verified CHECK asset_verified IN ('verified', 'unknown', 'malicious'),
    CONSTRAINT valid_asset_type CHECK asset_type IN ('native', 'token')
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (network, asset_contract);

-- Create index for efficient lookups by symbol
CREATE INDEX IF NOT EXISTS idx_assets_symbol ON assets (network, asset_symbol);

-- Create index for filtering by asset type
CREATE INDEX IF NOT EXISTS idx_assets_type ON assets (network, asset_type);

-- Create index for filtering by verification status
CREATE INDEX IF NOT EXISTS idx_assets_verified ON assets (network, asset_verified);

-- Insert native assets for supported networks
-- These are the primary native assets for each chain
INSERT INTO assets (network, asset_symbol, asset_contract, asset_verified, asset_name, asset_type, decimals, first_seen_block, first_seen_timestamp)
VALUES
    -- Torus native asset
    ('torus', 'TOR', 'native', 'verified', 'Torus', 'native', 18, 0, now()),
    
    -- Bittensor native asset
    ('bittensor', 'TAO', 'native', 'verified', 'Bittensor', 'native', 9, 0, now()),
    
    -- Polkadot native asset
    ('polkadot', 'DOT', 'native', 'verified', 'Polkadot', 'native', 10, 0, now());

-- Note: Additional assets (tokens) will be added dynamically by the indexers
-- as they are discovered on-chain