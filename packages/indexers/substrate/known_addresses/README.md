# Known Addresses Module

The Known Addresses module manages a database of known blockchain addresses with their labels and metadata. It provides functionality to import, store, and query labeled addresses for various blockchain networks.

## Overview

This module helps identify and label important addresses on blockchain networks, such as:
- Exchange wallets
- Protocol treasuries
- Notable individual wallets
- Smart contract addresses
- System addresses

## Architecture

### Components

1. **KnownAddressesImporter**: Main class for importing known addresses from external sources
2. **import_service.py**: Service layer for managing imports
3. **import_known_addresses.py**: CLI script for manual imports

### Database Schema

The module uses two tables:

#### known_addresses
Stores the actual address information:
- `id`: Unique identifier (UUID)
- `network`: Blockchain network (e.g., 'bittensor', 'torus', 'polkadot')
- `address`: The blockchain address
- `label`: Human-readable label
- `source`: Where the address info came from
- `source_type`: Type of source (external/internal)
- `last_updated`: Timestamp of last update
- `_version`: Version for deduplication

#### known_addresses_updates
Tracks when addresses were last updated for each network:
- `network`: Blockchain network identifier
- `last_updated`: Last update timestamp

## Features

- **GitHub Integration**: Automatically imports addresses from GitHub repositories
- **Version Control**: Uses ReplacingMergeTree for efficient updates
- **Multi-Network Support**: Handles addresses from different blockchain networks
- **Configurable Updates**: Set update intervals via environment variables

## Configuration

### Environment Variables

```bash
# GitHub repository URL containing known addresses
KNOWN_ADDRESSES_GITHUB_URL=https://raw.githubusercontent.com/your-org/known-addresses/main

# Update interval in seconds (default: 3600 = 1 hour)
KNOWN_ADDRESSES_UPDATE_INTERVAL=3600
```

### Expected GitHub Repository Structure

The GitHub repository should contain JSON files named `{network}_known_addresses.json`:

```json
[
  {
    "address": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
    "label": "Alice's Wallet",
    "source": "github",
    "source_type": "external"
  },
  {
    "address": "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
    "label": "Bob's Exchange Account",
    "source": "github",
    "source_type": "external"
  }
]
```

## Usage

### Importing Addresses

```python
from packages.indexers.substrate.known_addresses.known_addresses import KnownAddressesImporter

# Initialize importer
importer = KnownAddressesImporter({
    'host': 'localhost',
    'port': 9000,
    'user': 'default',
    'password': '',
    'database': 'substrate'
})

# Import addresses for a specific network
success = importer.import_from_github('bittensor')
```

### CLI Import

```bash
python packages/indexers/substrate/known_addresses/import_known_addresses.py \
    --network bittensor \
    --host localhost \
    --port 9000 \
    --database substrate
```

### Querying Known Addresses

```sql
-- Get all known addresses for a network
SELECT address, label, source, last_updated
FROM known_addresses
WHERE network = 'bittensor'
ORDER BY label;

-- Find a specific address
SELECT label, source, last_updated
FROM known_addresses
WHERE network = 'bittensor' 
  AND address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY';

-- Get addresses by source
SELECT address, label
FROM known_addresses
WHERE network = 'torus' 
  AND source = 'github'
ORDER BY last_updated DESC;

-- Check last update time for a network
SELECT last_updated
FROM known_addresses_updates
WHERE network = 'bittensor';
```

## Integration with Other Services

The known addresses can be joined with other tables to enrich blockchain data:

```sql
-- Example: Get labeled balance changes
SELECT 
    bt.address,
    ka.label,
    bt.balance_change,
    bt.block_height
FROM balance_tracking bt
LEFT JOIN known_addresses ka 
    ON bt.address = ka.address 
    AND ka.network = 'bittensor'
WHERE bt.block_height > 1000000
  AND ka.label IS NOT NULL;
```

## Best Practices

1. **Regular Updates**: Configure appropriate update intervals based on how frequently addresses change
2. **Source Management**: Use consistent source naming for tracking data provenance
3. **Label Standards**: Establish naming conventions for labels (e.g., "Exchange: Binance Hot Wallet")
4. **Network Consistency**: Ensure network names match across all services

## Extending the Module

To add new import sources:

1. Create a new method in `KnownAddressesImporter`
2. Define the data format and parsing logic
3. Add appropriate error handling
4. Update the import service to support the new source

Example:
```python
def import_from_csv(self, network: str, csv_path: str) -> bool:
    """Import addresses from a CSV file"""
    # Implementation here
    pass