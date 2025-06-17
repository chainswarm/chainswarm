# Clarification: Transaction Frequency Tracking

## Current Plan (Gap-based)
```cypher
(:Address)-[:TO {
    // Only stores gaps between transfers
    tx_frequency: [3600000, 7200000, 1800000],  // Time gaps in ms
    block_frequency: [10, 20, 5],                // Block gaps
}]->(:Address)
```

## Alternative: Full History Tracking
```cypher
(:Address)-[:TO {
    // Store complete history
    transfer_timestamps: [1701432000000, 1701435600000, 1701442800000],
    transfer_blocks: [1000, 1010, 1030],
    transfer_amounts: [100.5, 200.3, 150.7],
    
    // Still calculate gaps for quick access
    tx_frequency: [3600000, 7200000],  // Derived from timestamps
    block_frequency: [10, 20],          // Derived from blocks
}]->(:Address)
```

## Pros and Cons

### Gap-based (Current Plan)
**Pros:**
- Smaller storage footprint
- Direct access to frequency metrics
- Good for pattern detection

**Cons:**
- Can't reconstruct exact transfer times
- Limited historical analysis

### Full History
**Pros:**
- Complete transfer history
- Can analyze specific time periods
- Can correlate with external events

**Cons:**
- Much larger storage (arrays can grow very large)
- Need to calculate gaps on the fly
- Performance impact on large edges

## Hybrid Approach (Recommended)
```cypher
(:Address)-[:TO {
    // Basic metrics
    asset: "TOR",
    volume: 1000,
    transfer_count: 5,
    
    // First and last transfers
    first_transfer_timestamp: 1701432000000,
    last_transfer_timestamp: 1701450000000,
    first_transfer_block: 1000,
    last_transfer_block: 1050,
    
    // Recent history (last N transfers)
    recent_timestamps: [1701446400000, 1701450000000],  // Last 10-20
    recent_blocks: [1040, 1050],
    recent_amounts: [75.5, 125.0],
    
    // Frequency metrics (calculated)
    tx_frequency: [3600000, 7200000, 1800000, 3600000],
    avg_time_gap: 4050000,
    transfer_pattern: "regular"
}]->(:Address)
```

This gives us:
- Full history for recent transfers
- Frequency metrics for pattern analysis
- Bounded storage growth
- Ability to analyze recent activity in detail