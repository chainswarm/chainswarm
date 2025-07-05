from decimal import Decimal, ROUND_HALF_UP
from typing import Union, Dict, Optional

# Define network token decimals in a single place
NETWORK_TOKEN_DECIMALS: Dict[str, int] = {
    'torus': 18,
    'torus_testnet': 18,
    'bittensor': 18,
    'bittensor_testnet': 18,
    'polkadot': 10,
    # Add other networks as needed
}

# Define asset-specific decimal places
ASSET_DECIMALS: Dict[str, int] = {
    'TOR': 18,  # Torus native token
    'TAO': 18,  # Bittensor native token
    'DOT': 10,  # Polkadot native token
}

# Default target precision for Memgraph and API responses
TARGET_PRECISION = 8

def get_network_token_decimals(network: str) -> int:
    """
    Get the number of decimal places for a network's native token.
    
    Args:
        network: The blockchain network identifier (e.g., 'torus', 'bittensor', 'polkadot')
        
    Returns:
        Number of decimal places for the network's native token
    """
    return NETWORK_TOKEN_DECIMALS.get(network.lower(), 18)

def get_asset_decimals(asset: str) -> int:
    """
    Get the number of decimal places for a specific asset.
    
    This function provides asset-specific decimal information, which is essential
    for proper handling of different tokens across various blockchain networks.
    Each asset may have different decimal precision requirements.
    
    Args:
        asset (str): The asset symbol (e.g., 'TOR', 'TAO', 'DOT')
        
    Returns:
        int: Number of decimal places for the asset
             Returns 18 as default if asset is not found
             
    Examples:
        >>> get_asset_decimals('TOR')
        18
        >>> get_asset_decimals('DOT')
        10
    """
    return ASSET_DECIMALS.get(asset.upper(), 18)

def get_decimals_for_network_asset(network: str, asset: Optional[str] = None) -> int:
    """
    Get decimal places for an asset on a specific network.
    
    This function provides a unified way to get decimal information by either
    using the provided asset symbol or deriving it from the network. It ensures
    consistency between network-based and asset-based decimal lookups.
    
    Args:
        network (str): The blockchain network identifier
        asset (Optional[str]): The asset symbol. If None, uses network's native asset
        
    Returns:
        int: Number of decimal places for the asset
        
    Examples:
        >>> get_decimals_for_network_asset('torus')
        18
        >>> get_decimals_for_network_asset('polkadot', 'DOT')
        10
    """
    if asset:
        return get_asset_decimals(asset)
    return get_network_token_decimals(network)

def to_8_digit_precision(
    amount: Union[str, int, float, Decimal],
    network: str,
    asset: Optional[str] = None
) -> int:
    """
    Convert an amount with network-specific or asset-specific precision to 8 decimal places
    for storage in Memgraph and API responses.
    
    This function now supports asset-aware conversions while maintaining backward
    compatibility with network-only usage. When an asset is provided, it uses
    asset-specific decimal information; otherwise, it falls back to network defaults.
    
    Args:
        amount: Amount to convert (string, int, float, or Decimal)
        network: Blockchain network identifier
        asset: Optional asset symbol (e.g., 'TOR', 'TAO', 'DOT')
        
    Returns:
        int: Fixed-point integer representation with 8 decimal places
        
    Example:
        amount = "123.456789012345678901234567890" (with 18 decimals)
        -> converted to 8 decimals: 12345678901
        
        This represents 123.45678901 but is stored as an integer.
        
    Examples:
        >>> to_8_digit_precision("1000000000000000000", "torus")  # Backward compatible
        100000000
        >>> to_8_digit_precision("1000000000000000000", "torus", "TOR")  # Asset-aware
        100000000
    """
    source_decimals = get_decimals_for_network_asset(network, asset)
    
    # Convert amount to Decimal
    decimal_amount = Decimal(str(amount))
    
    # Calculate conversion factor
    factor = Decimal(10 ** (source_decimals - TARGET_PRECISION))
    
    # Convert to 8 decimal places and return as integer with proper rounding
    # Use quantize with 0 decimal places after dividing by the factor
    # This ensures proper rounding while maintaining 8 decimal places of precision
    return int((decimal_amount / factor).quantize(Decimal('0'), rounding=ROUND_HALF_UP))

def convert_to_decimal_units(
    amount: Union[str, int, float, Decimal],
    network: str = 'torus',
    asset: Optional[str] = None
) -> Decimal:
    """
    Convert raw blockchain amount to decimal units by dividing by 10^decimals.
    
    This function now supports asset-aware conversions while maintaining backward
    compatibility. When an asset is provided, it uses asset-specific decimal
    information; otherwise, it uses network-based defaults.
    
    Args:
        amount: The raw blockchain amount (string, int, float, or Decimal)
        network: The blockchain network identifier (e.g., 'torus', 'bittensor', 'polkadot')
        asset: Optional asset symbol (e.g., 'TOR', 'TAO', 'DOT')
        
    Returns:
        Decimal: The amount in decimal units
        
    Examples:
        amount = 10100000000000000000 (raw blockchain value)
        network = 'torus' (18 decimals)
        -> converted to decimal units: 10.1
        
        >>> convert_to_decimal_units("1000000000000000000", "torus")  # Backward compatible
        Decimal('1.0')
        >>> convert_to_decimal_units("1000000000000000000", "torus", "TOR")  # Asset-aware
        Decimal('1.0')
    """
    # Ensure the amount is a Decimal
    if not isinstance(amount, Decimal):
        amount = Decimal(str(amount))
    
    # Get the number of decimal places for this network/asset
    decimals = get_decimals_for_network_asset(network, asset)
    
    # Divide by 10^decimals to get the amount in decimal units
    divisor = Decimal(10) ** decimals
    
    return amount / divisor

def convert_from_decimal_units(
    amount: Union[str, int, float, Decimal],
    network: str = 'torus',
    asset: Optional[str] = None
) -> Decimal:
    """
    Convert decimal amount to raw blockchain units by multiplying by 10^decimals.
    
    This is the inverse operation of convert_to_decimal_units. It converts
    human-readable decimal amounts back to the raw blockchain representation.
    
    Args:
        amount: The decimal amount (string, int, float, or Decimal)
        network: The blockchain network identifier
        asset: Optional asset symbol (e.g., 'TOR', 'TAO', 'DOT')
        
    Returns:
        Decimal: The amount in raw blockchain units
        
    Examples:
        >>> convert_from_decimal_units("1.5", "torus", "TOR")
        Decimal('1500000000000000000')
        >>> convert_from_decimal_units("10.0", "polkadot", "DOT")
        Decimal('100000000000')
    """
    # Ensure the amount is a Decimal
    if not isinstance(amount, Decimal):
        amount = Decimal(str(amount))
    
    # Get the number of decimal places for this network/asset
    decimals = get_decimals_for_network_asset(network, asset)
    
    # Multiply by 10^decimals to get the raw blockchain units
    multiplier = Decimal(10) ** decimals
    
    return amount * multiplier

def format_asset_amount(
    amount: Union[str, int, float, Decimal],
    network: str,
    asset: Optional[str] = None,
    precision: Optional[int] = None
) -> str:
    """
    Format an asset amount for display with appropriate precision.
    
    This function converts raw blockchain amounts to human-readable format
    with proper decimal precision and asset-aware formatting.
    
    Args:
        amount: The raw blockchain amount
        network: The blockchain network identifier
        asset: Optional asset symbol
        precision: Optional display precision (defaults to 4 for readability)
        
    Returns:
        str: Formatted amount string
        
    Examples:
        >>> format_asset_amount("1500000000000000000", "torus", "TOR")
        '1.5000'
        >>> format_asset_amount("100000000000", "polkadot", "DOT", precision=2)
        '10.00'
    """
    if precision is None:
        precision = 4
    
    # Convert to decimal units
    decimal_amount = convert_to_decimal_units(amount, network, asset)
    
    # Format with specified precision
    return f"{decimal_amount:.{precision}f}"

def validate_asset_for_network(network: str, asset: str) -> bool:
    """
    Validate that an asset is the native asset for a given network.
    
    This function ensures that asset assignments are correct for their
    respective networks, helping prevent configuration errors.
    
    Args:
        network: The blockchain network identifier
        asset: The asset symbol to validate
        
    Returns:
        bool: True if the asset is valid for the network, False otherwise
        
    Examples:
        >>> validate_asset_for_network("torus", "TOR")
        True
        >>> validate_asset_for_network("torus", "TAO")
        False
    """
    try:
        # Import here to avoid circular imports
        from packages.indexers.substrate import get_native_network_asset
        expected_asset = get_native_network_asset(network)
        return asset.upper() == expected_asset.upper()
    except (ImportError, ValueError):
        # Fallback validation using direct mapping
        network_asset_map = {
            'torus': 'TOR',
            'torus_testnet': 'TOR',
            'bittensor': 'TAO',
            'bittensor_testnet': 'TAO',
            'polkadot': 'DOT'
        }
        expected_asset = network_asset_map.get(network.lower())
        return expected_asset and asset.upper() == expected_asset.upper()