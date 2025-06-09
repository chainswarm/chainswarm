from typing import List, Dict, Any

from packages.api.services.balance_tracking_service import BalanceTrackingService


def add_searched_badge_to_nodes(result: List[Dict[str, Any]], searched_addresses: List[str]) -> List[Dict[str, Any]]:
    """
    Helper function to add 'searched' badge to nodes that match the searched addresses.
    
    Args:
        result: The result from the money flow service
        searched_addresses: List of addresses to mark as searched
        
    Returns:
        The enriched result with 'searched' badges added
    """
    if not result:
        return result
        
    for item in result:
        if item.get('element', {}).get('type') == 'node':
            node_address = item.get('element', {}).get('address')
            if node_address in searched_addresses:
                # Add 'searched' badge to the node
                badges = item['element'].get('badges', [])
                if 'searched' not in badges:
                    badges.append('searched')
                    item['element']['badges'] = badges
                    
    return result


def enrich_nodes_with_balances(balance_service: BalanceTrackingService, result: List[Dict[str, Any]], network: str, assets: List[str] = None) -> List[Dict[str, Any]]:
    """
    Enrich nodes with balance information by querying the latest balances.
    
    Args:
        balance_service: The balance service instance to query balances
        result: The result from the money flow service
        network: The blockchain network identifier
        assets: List of assets to filter balances by

    Returns:
        The enriched result with balance information added to nodes
    """
    if not result:
        return result
        
    # Extract all addresses from nodes
    addresses = []
    address_to_node = {}
    
    for item in result:
        if item.get('element', {}).get('type') == 'node':
            node_address = item.get('element', {}).get('address')
            if node_address:
                addresses.append(node_address)
                address_to_node[node_address] = item['element']
    
    if not addresses:
        return result
    
    # Process addresses in batches
    batch_size = 10000
    address_batches = [addresses[i:i + batch_size] for i in range(0, len(addresses), batch_size)]

    # Query balances for each batch and add to nodes
    for batch in address_batches:
        balances = balance_service.get_latest_balances_for_addresses(batch, assets)
        
        # First ensure all nodes have a balance property initialized to 0
        for address in addresses:
            if address in address_to_node and 'balance' not in address_to_node[address]:
                address_to_node[address]['balance'] = 0
                address_to_node[address]['balance_timestamp'] = 0
        
        # Add balance to each node that has data
        for address, balance_data in balances.items():
            if address in address_to_node:
                # Use balance directly as it's already in human-readable format
                balance = balance_data.get('balance', 0)
                timestamp = balance_data.get('timestamp', 0)
                
                # Add balance to node
                address_to_node[address]['balance'] = balance
                address_to_node[address]['balance_timestamp'] = timestamp
    
    return result