# Installing and Configuring mcp-proxy

This guide provides instructions for installing and configuring the `mcp-proxy` package, which is required to connect Claude desktop to the Torus blockchain.

## What is mcp-proxy?

`mcp-proxy` is a Python package that implements the Model Context Protocol (MCP). It serves as a bridge between Claude and external services, allowing Claude to access and interact with the Torus blockchain data and functionality.

The package is available at: https://github.com/sparfenyuk/mcp-proxy

## System Requirements

- Windows 10 or Windows 11
- Python 3.8 or higher
- pip (Python package installer)
- Claude desktop application
- Internet connection to access the Torus blockchain endpoint

## Installation Steps

### 1. Install the mcp-proxy Package

The `mcp-proxy` package can be installed using pip:

```powershell
pip install mcp-proxy
```

This will install the package and its dependencies, and make the `mcp-proxy` executable available in your Python environment.

### 2. Verify the Installation

After installation, you can verify that the package was installed correctly by running:

```powershell
mcp-proxy --version
```

If the installation was successful, this command should display the version information of the installed package.

## Configuration for Claude Desktop

After installing the `mcp-proxy` package, you need to configure Claude desktop to use it:

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

## Troubleshooting Installation Issues

### Package Installation Failures

If you encounter issues installing the package:

```powershell
# Try installing with verbose output
pip install -v mcp-proxy

# If you have permission issues, try installing for the current user only
pip install --user mcp-proxy

# If you need to upgrade an existing installation
pip install --upgrade mcp-proxy
```

### Executable Not Found

If Claude reports that it cannot find the `mcp-proxy` executable:

1. Ensure the package is installed correctly
2. Check if the executable is in your PATH
3. Try using the full path to the executable in the Claude configuration

You can find the location of the installed executable by running:

```powershell
where mcp-proxy
```

### Connection Issues

If `mcp-proxy` cannot connect to the Torus endpoint:

1. Check your internet connection
2. Verify that the endpoint URL is correct
3. Check if your firewall is blocking the connection
4. Try using a different network connection

## Security Considerations

When using `mcp-proxy`, keep the following security considerations in mind:

1. **API Endpoints**: Only connect to trusted API endpoints
2. **Updates**: Regularly update the `mcp-proxy` package using `pip install --upgrade`
3. **Permissions**: Run with minimal required permissions
4. **Network Security**: Be aware that the proxy communicates over the network

## Additional Resources

For more information about the Model Context Protocol and how to use `mcp-proxy`, refer to:

- [Claude Integration Guide](./claude_integration.md)
- [Torus Blockchain Interaction Guide](./torus_blockchain_interaction.md)
- [mcp-proxy GitHub Repository](https://github.com/sparfenyuk/mcp-proxy)
- [Anthropic MCP Documentation](https://docs.anthropic.com/claude/docs/model-context-protocol)