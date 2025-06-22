"""
Balance Volume Series Indexer

This module provides time-series tracking of transfer volumes at fixed intervals.
It analyzes transaction volumes and patterns to enable better understanding of 
network activity and token flow.

Key features:
- Records transfer volumes at fixed intervals (e.g., 4-hour periods)
- Categorizes transactions by size (micro, small, medium, large, whale)
- Tracks network activity metrics like unique senders/receivers and density
- Provides time-series views for trend analysis (daily, weekly, monthly)
- Enables volume-based analysis of network activity patterns
"""

from packages.indexers.substrate.balance_volume_series.balance_volume_series_indexer_base import BalanceVolumeSeriesIndexerBase
from packages.indexers.substrate.balance_volume_series.balance_volume_series_consumer import (
    BalanceVolumeSeriesConsumer, 
    get_balance_volume_series_indexer
)

__all__ = [
    'BalanceVolumeSeriesIndexerBase',
    'BalanceVolumeSeriesConsumer', 
    'get_balance_volume_series_indexer'
]