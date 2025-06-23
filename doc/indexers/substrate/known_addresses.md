# Known Addresses Service

## Overview

The Known Addresses service provides a system for storing, managing, and retrieving labeled blockchain addresses across Substrate networks. It functions as a standalone service that maps cryptographic addresses to human-readable labels and metadata. The service stores data in the database that is accessed directly by the API layer (REST or MCP), not by other indexers. It offers the following key features:

- **Address labeling** for improved readability and identification
- **Metadata management** including source tracking and categorization
- **Versioned updates** to maintain data consistency
- **External data integration** from repository sources
- **Efficient querying** with pagination and filtering capabilities

## Core Table Structure

The foundation of the indexer consists of two tables:

```sql
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
```

### Key Fields

#### Known Addresses Table
- **id**: UUID that uniquely identifies each address entry
- **network**: The blockchain network this address belongs to (e.g., 'bittensor', 'torus')
- **address**: The actual blockchain address (public key)
- **label**: Human-readable name or description for the address
- **source**: Where this address information came from (e.g., 'github', 'manual')
- **source_type**: Categorization of the source (e.g., 'external', 'internal')
- **last_updated**: Timestamp when this entry was last modified
- **_version**: Used for versioning with the ReplacingMergeTree engine

#### Updates Tracking Table
- **network**: The blockchain network identifier
- **last_updated**: Timestamp when addresses for this network were last updated

## Address Labeling and Metadata Management

The Known Addresses service provides a comprehensive system for managing blockchain address metadata:

### Address Labeling
- Addresses are mapped to human-readable labels for easier identification
- Labels can represent entities, services, exchanges, or other meaningful identifiers
- This mapping enables more intuitive analysis of blockchain activity

### Metadata Sources
- **Source tracking**: Each address entry includes information about where the data originated
- **Source types**: Addresses can be categorized by source type (external vs. internal)
- **Multiple sources**: The same address can have different labels from different sources

### Data Integrity
- The composite primary key `(network, address, source)` ensures uniqueness
- This allows the same address to have different labels from different sources
- The system maintains consistency while allowing for diverse metadata

## Update Tracking Mechanism

The Known Addresses service implements a robust update mechanism:

### Versioned Updates
- The `_version` field combined with the ReplacingMergeTree engine enables versioned updates
- When an address is updated, a new version is created with an incremented version number
- During queries, only the latest version is returned when using the `FINAL` keyword

### Network-Level Update Tracking
- The `known_addresses_updates` table tracks when each network's addresses were last updated
- This enables efficient scheduling of periodic updates
- It also provides visibility into the freshness of address data

### Update Process
1. New address data is fetched from external sources (e.g., GitHub repositories)
2. Each address is inserted with a new version number
3. Addresses no longer present in the source are identified and removed
4. The network's last update timestamp is refreshed

## Common Query Patterns

### Retrieving Known Addresses by Network

```sql
-- Get all known addresses for a specific network
SELECT 
    address,
    label,
    source,
    source_type,
    last_updated
FROM known_addresses FINAL
WHERE network = 'torus'
ORDER BY label, address
LIMIT 100
```

### Looking Up Specific Addresses

```sql
-- Look up labels for specific addresses
SELECT 
    address,
    label,
    source,
    source_type
FROM known_addresses FINAL
WHERE 
    network = 'bittensor' AND
    address IN ('5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY', '5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty')
```

### Paginated Address Retrieval

```sql
-- Get addresses with pagination
SELECT 
    id,
    network,
    address,
    label,
    source,
    source_type,
    last_updated
FROM known_addresses FINAL
WHERE network = 'torus'
ORDER BY label, address
LIMIT 100 OFFSET 200  -- Page 3 with page_size=100
```

### Checking Update Status

```sql
-- Check when addresses were last updated
SELECT 
    network,
    last_updated
FROM known_addresses_updates
WHERE network IN ('torus', 'bittensor', 'polkadot')
```

## Implementation Details

### Data Import Process

The Known Addresses service includes a dedicated import service that:

1. Fetches address data from configured repository sources
2. Parses and validates the address information
3. Assigns unique IDs and timestamps to new entries
4. Inserts the data with appropriate versioning
5. Removes outdated entries no longer present in the source
6. Updates the network's last update timestamp

### API Integration

The system provides a REST API for accessing known addresses:

- **GET /{network}/known-addresses**: Retrieves known addresses with pagination
- Optional filtering by specific addresses
- Standardized pagination with metadata (page, page_size, total_items, total_pages)

### Relationship with Other Indexers

The Known Addresses service operates as a completely standalone service that is separate from the indexer data flow. Unlike the Balance Transfers, Balance Series, and Money Flow indexers which consume data from the Block Stream indexer, the Known Addresses service:

1. Maintains its own independent data source and update cycle
2. Stores data that is accessed directly by the API layer (REST or MCP)
3. Does not depend on the Block Stream indexer for its core functionality
4. Is not used by other indexers

### Usage in Other Indexers

The Known Addresses service provides value to the API layer by:

1. Storing human-readable labels for addresses that can be accessed by API endpoints
2. Enabling more intuitive presentation of blockchain data to end users
3. Supporting the identification of significant entities when queried through the API
4. Improving the readability of reports and visualizations generated from API data