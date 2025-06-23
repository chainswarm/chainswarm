# Interacting with Torus Blockchain via Claude

This guide provides detailed instructions on how to interact with the Torus blockchain using Claude desktop after configuring the MCP server.

## MCP Server Configuration Details

The MCP server configuration connects Claude to the Torus blockchain services through the `mcp-proxy` Python package. Here's a breakdown of the configuration:

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

- `torus-chainswarm`: The identifier for the MCP server
- `command`: The command to run the mcp-proxy executable (installed via pip)
- `args`: Arguments passed to the executable, including the Torus blockchain endpoint URL

## Getting Started with Claude and Torus

After configuring Claude desktop with the MCP server, follow these steps to begin interacting with the Torus blockchain:

1. **Initialize the connection**:
   - Start a new conversation in Claude
   - Type: "get user guidance"
   - Claude will respond with an overview of available blockchain tools and capabilities

2. **Access detailed instructions**:
   - Type: "read instructions"
   - Claude will provide comprehensive documentation on available commands and query formats

3. **Begin blockchain interaction**:
   - You can now use natural language to query the Torus blockchain
   - Claude will translate your requests into appropriate API calls via the MCP server

## Example Interactions

Here are some examples of how you can interact with the Torus blockchain through Claude:

### Querying Account Balances

```
What is the current balance of address 5F3sa2TJAWMqDhXG6jhV4N8ko9SxwGy8TpaNS1repo5EYjQX?
```

### Tracking Token Transfers

```
Show me the last 5 token transfers involving address 5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY
```

### Analyzing Money Flow

```
Analyze the money flow pattern for address 5DAAnrj7VHTznn2AWBemMuyBwZWs6FNFjdyVXUeYum3PTXFy over the last 30 days
```

### Searching for Specific Transactions

```
Find all transactions with values greater than 1000 TORUS in the last week
```

## Advanced Features

### Custom Time Ranges

You can specify custom time ranges for your queries:

```
Show balance changes for address 5GNJqTPyNqANBkUVMN1LPPrxXnFouWXoe2wNSmmEoLctxiZY between June 1, 2025 and June 15, 2025
```

### Comparative Analysis

Compare activity between different addresses:

```
Compare transaction volumes between addresses 5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY and 5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty
```

### Network Statistics

Get overall network statistics:

```
What is the current transaction volume on the Torus network?
```

## Troubleshooting Common Issues

### Connection Problems

If Claude indicates it cannot connect to the Torus blockchain:

1. Verify your internet connection
2. Check that the Torus endpoint is accessible
3. Ensure the `mcp-proxy` is running correctly

### Query Timeouts

For complex queries that time out:

1. Try breaking your query into smaller, more specific requests
2. Specify narrower time ranges
3. Limit the number of transactions or addresses in a single query

### Incorrect Results

If you receive unexpected results:

1. Verify the address format is correct
2. Check for typos in your query
3. Try rephrasing your request using different terminology

## Best Practices

1. **Be specific**: Clearly specify addresses, time periods, and the type of information you need
2. **Start simple**: Begin with basic queries before moving to more complex analyses
3. **Use natural language**: You don't need to use technical commands; Claude understands natural language requests
4. **Provide context**: When analyzing specific addresses, provide context about what you're looking for

## Security Considerations

- The MCP server only provides read-only access to blockchain data
- No private keys or sensitive information should ever be shared with Claude
- All queries are processed through the secure ChainSwarm API