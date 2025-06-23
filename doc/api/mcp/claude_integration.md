# Configuring MCP Server with Claude Desktop

This guide explains how to set up and configure an MCP (Model Context Protocol) server to be used with Claude desktop, allowing you to interact with the Torus blockchain through Claude.

## What is MCP?

The Model Context Protocol (MCP) enables AI assistants like Claude to access external tools and resources. By configuring an MCP server, you can extend Claude's capabilities to interact with blockchain data and perform specialized operations.

## Installation Steps

### 1. Install mcp-proxy

The `mcp-proxy` Python package acts as a bridge between Claude and the Torus blockchain services.

1. Install the package using pip:
   ```powershell
   pip install git+https://github.com/sparfenyuk/mcp-proxy.git
   ```

2. Verify the installation:
   ```powershell
   mcp-proxy --version
   ```

### 2. Configure Claude Desktop

To enable Claude to use the MCP server, you need to add the server configuration to Claude's settings:

1. Open Claude desktop application
2. Navigate to Settings > Advanced Settings
3. Find the "MCP Servers" section
4. Add the following configuration:

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

5. Save the settings and restart Claude desktop

> **Note**: If the `mcp-proxy` command is not found, you may need to use the full path to the executable. You can find the location by running `where mcp-proxy` in PowerShell.

## Using the MCP Server with Claude

After configuring the MCP server and restarting Claude, follow these steps to interact with the Torus blockchain:

1. Start a new conversation in Claude
2. Type "get user guidance" to receive initial instructions on available blockchain tools
3. Type "read instructions" to get detailed information about how to interact with the Torus blockchain
4. You can now communicate with the Torus blockchain through Claude using natural language

## Available Blockchain Operations

Once connected, you can perform various operations on the Torus blockchain, including:

- Querying account balances
- Tracking token transfers
- Monitoring blockchain activity
- Analyzing money flow patterns
- Searching for specific transactions

## Troubleshooting

If you encounter issues with the MCP server connection:

1. Verify that `mcp-proxy.exe` is correctly installed in the specified path
2. Check that the Claude configuration contains the correct path to the executable
3. Ensure that the Torus blockchain endpoint is accessible from your network
4. Restart Claude desktop after making any configuration changes

## Additional Resources

For more information about the Torus blockchain and available API endpoints, refer to the other documentation sections:

- [Balance Transfers](/doc/indexers/substrate/balance_transfers.md)
- [Money Flow Analysis](/doc/indexers/substrate/money_flow.md)
- [Known Addresses](/doc/indexers/substrate/known_addresses.md)