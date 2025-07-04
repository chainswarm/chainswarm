# Claude Desktop MCP Integration for Torus Blockchain

This documentation explains how to set up and use the Model Context Protocol (MCP) integration between Claude desktop and the Torus blockchain.

## Overview

The Model Context Protocol (MCP) allows Claude to access external tools and resources, extending its capabilities beyond conversation. This integration enables Claude to interact with the Torus blockchain, providing you with powerful blockchain analysis and query capabilities through natural language.

## Documentation Guides

This documentation is organized into several guides:

1. [**Claude Integration Guide**](./claude_integration.md) - Overview of setting up Claude desktop with MCP server
2. [**MCP Proxy Installation**](./mcp_proxy_installation.md) - Detailed instructions for installing and configuring mcp-proxy
3. [**Torus Blockchain Interaction**](./torus_blockchain_interaction.md) - Guide to interacting with the Torus blockchain through Claude

## Quick Start Guide

For those who want to get started quickly:

1. **Install mcp-proxy**:
   ```powershell
   # Install the mcp-proxy Python package
   pip install mcp-proxy
   ```

2. **Configure Claude desktop**:
   - Open Claude desktop → Settings → Advanced Settings
   - Add this configuration:
   ```json
   {
     "mcpServers": {
       "torus-chainswarm": {
         "command": "mcp-proxy",
         "args": [
           "https://torus.chainswarm.ai/mcp/sse"
         ]
       }
     }
   }
   ```
   - Restart Claude desktop

3. **Start using the integration**:
   - Type "get user guidance" in a new conversation
   - Type "read instructions" to get detailed usage information
   - Begin interacting with the Torus blockchain using natural language

## Key Features

With this integration, you can:

- Query account balances and transaction history
- Track token transfers and analyze money flow
- Monitor blockchain activity in real-time
- Perform complex blockchain data analysis
- Search for specific transactions or patterns

## Requirements

- Windows 10 or Windows 11
- Claude desktop application
- Internet connection to access the Torus blockchain
- PowerShell 5.1 or later

## Related Resources

- [Balance Transfers Documentation](/docs/indexers/substrate/balance_transfers.md)
- [Money Flow Analysis](/docs/indexers/substrate/money_flow.md)
- [Known Addresses](/docs/indexers/substrate/known_addresses.md)
- [Block Stream](/docs/indexers/substrate/block_stream.md)

## Support

If you encounter any issues with the MCP integration:

1. Check the troubleshooting sections in the individual guides
2. Verify your network connection and firewall settings
3. Ensure you're using the latest version of mcp-proxy
4. Contact ChainSwarm support if issues persist