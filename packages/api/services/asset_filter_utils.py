from typing import List


def build_asset_filter(assets: List[str]) -> str:
    """Build SQL filter for assets, checking both symbol and contract fields
    
    This function creates a flexible filter that matches values against both
    the asset (symbol) and asset_contract fields, allowing users to query by:
    - Symbol: assets=["TOR"] - finds all TOR assets
    - Contract: assets=["0x123..."] - finds specific contract
    - Native: assets=["native"] - finds all native assets
    - Mixed: assets=["TOR", "0x456..."] - finds TOR assets OR specific contract
    
    Args:
        assets: List of assets to filter by (can be symbols, contract addresses, or 'native')
        
    Returns:
        SQL filter string (including the AND prefix) or empty string if no filter needed
    """
    if not assets or assets == ["all"]:
        return ""
    
    asset_conditions = []
    for asset in assets:
        # For each value, check if it matches either asset (symbol) OR asset_contract
        # This allows users to query without knowing whether it's a symbol or contract
        asset_conditions.append(f"(asset = '{asset}' OR asset_contract = '{asset}')")
    
    return f" AND ({' OR '.join(asset_conditions)})"