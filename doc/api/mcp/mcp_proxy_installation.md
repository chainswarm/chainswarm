# Installing and Configuring mcp-proxy.exe

This guide provides detailed instructions for installing and configuring the `mcp-proxy.exe` executable, which is required to connect Claude desktop to the Torus blockchain.

## What is mcp-proxy.exe?

`mcp-proxy.exe` is a proxy executable that implements the Model Context Protocol (MCP). It serves as a bridge between Claude and external services, allowing Claude to access and interact with the Torus blockchain data and functionality.

## System Requirements

- Windows 10 or Windows 11
- PowerShell 5.1 or later
- .NET Framework 4.7.2 or later
- Internet connection to access the Torus blockchain endpoint

## Installation Steps

### 1. Create the Installation Directory

First, create the directory where `mcp-proxy.exe` will be installed:

```powershell
# Create the .local/bin directory in your user profile if it doesn't exist
$binPath = "$env:USERPROFILE\.local\bin"
if (-not (Test-Path $binPath)) {
    New-Item -ItemType Directory -Path $binPath -Force
}
```

### 2. Download mcp-proxy.exe

Download the latest version of `mcp-proxy.exe` from the official source:

```powershell
# Define the download URL and destination path
$downloadUrl = "https://github.com/chainswarm/mcp-proxy/releases/latest/download/mcp-proxy.exe"
$proxyPath = "$binPath\mcp-proxy.exe"

# Download the file
Invoke-WebRequest -Uri $downloadUrl -OutFile $proxyPath
```

Alternatively, you can manually download the file from the ChainSwarm website and place it in the `C:\Users\{user_name}\.local\bin\` directory.

### 3. Verify the Installation

Verify that `mcp-proxy.exe` is correctly installed and accessible:

```powershell
# Test the executable
& "$env:USERPROFILE\.local\bin\mcp-proxy.exe" --version
```

This should display the version information of the installed `mcp-proxy.exe`.

### 4. Add to PATH (Optional)

For easier access, you can add the `.local\bin` directory to your system PATH:

```powershell
# Add to PATH for the current session
$env:PATH += ";$env:USERPROFILE\.local\bin"

# Add to PATH permanently
[Environment]::SetEnvironmentVariable(
    "PATH",
    [Environment]::GetEnvironmentVariable("PATH", "User") + ";$env:USERPROFILE\.local\bin",
    "User"
)
```

## Configuration for Claude Desktop

After installing `mcp-proxy.exe`, you need to configure Claude desktop to use it:

1. Open Claude desktop application
2. Navigate to Settings > Advanced Settings
3. Find the "MCP Servers" section
4. Add the following configuration (replace `{user_name}` with your Windows username):

```json
{
  "mcpServers": {
    "torus-chainswarm": {
      "command": "C:\\Users\\{user_name}\\.local\\bin\\mcp-proxy.exe",
      "args": [
        "https://torus.chainswarm.ai/mcp/sse"
      ]
    }
  }
}
```

5. Save the settings and restart Claude desktop

## Configuration Options

The `mcp-proxy.exe` executable accepts several configuration options:

### Command-line Arguments

- `--help`: Display help information
- `--version`: Display version information
- `--verbose`: Enable verbose logging
- `--timeout <seconds>`: Set request timeout (default: 30 seconds)

### Environment Variables

You can also configure `mcp-proxy.exe` using environment variables:

- `MCP_PROXY_TIMEOUT`: Set request timeout in seconds
- `MCP_PROXY_LOG_LEVEL`: Set log level (debug, info, warn, error)
- `MCP_PROXY_API_KEY`: Set API key for authenticated endpoints

## Troubleshooting Installation Issues

### Executable Not Found

If Claude reports that it cannot find the `mcp-proxy.exe` executable:

1. Verify the path in the Claude configuration matches the actual location of the executable
2. Check that the executable has the correct permissions
3. Try using the full absolute path without environment variables

### Connection Issues

If `mcp-proxy.exe` cannot connect to the Torus endpoint:

1. Check your internet connection
2. Verify that the endpoint URL is correct
3. Check if your firewall is blocking the connection
4. Try using a different network connection

### Permission Issues

If you encounter permission issues:

1. Run PowerShell as Administrator when installing
2. Check that the user has write permissions to the `.local\bin` directory
3. Verify that the executable has the necessary execution permissions

## Security Considerations

When using `mcp-proxy.exe`, keep the following security considerations in mind:

1. **API Endpoints**: Only connect to trusted API endpoints
2. **Updates**: Regularly update `mcp-proxy.exe` to the latest version
3. **Permissions**: Run with minimal required permissions
4. **Network Security**: Be aware that the proxy communicates over the network

## Updating mcp-proxy.exe

To update `mcp-proxy.exe` to the latest version:

```powershell
# Download the latest version
$downloadUrl = "https://github.com/chainswarm/mcp-proxy/releases/latest/download/mcp-proxy.exe"
$proxyPath = "$env:USERPROFILE\.local\bin\mcp-proxy.exe"

# Backup the existing file
if (Test-Path $proxyPath) {
    Rename-Item -Path $proxyPath -NewName "mcp-proxy.exe.bak" -Force
}

# Download the new version
Invoke-WebRequest -Uri $downloadUrl -OutFile $proxyPath
```

## Additional Resources

For more information about the Model Context Protocol and how to use `mcp-proxy.exe`, refer to:

- [Claude Integration Guide](./claude_integration.md)
- [Torus Blockchain Interaction Guide](./torus_blockchain_interaction.md)
- [MCP Protocol Documentation](https://github.com/anthropics/anthropic-cookbook/tree/main/mcp)