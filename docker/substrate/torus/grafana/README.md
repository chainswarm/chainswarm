# Grafana Configuration for Torus Infrastructure

This directory contains the Grafana configuration for the Torus infrastructure monitoring setup.

## Directory Structure

```
grafana/
├── provisioning/
│   ├── datasources/
│   │   └── datasources.yml          # Prometheus and Loki datasource configuration
│   └── dashboards/
│       ├── dashboard.yml            # Dashboard provider configuration
│       ├── api-monitoring.json      # API performance and health monitoring
│       ├── chainswarm-overview.json # High-level system overview
│       ├── database-performance.json # Database metrics and performance
│       ├── indexer-performance.json # Indexer service monitoring
│       ├── logs-monitoring.json     # Log aggregation and analysis
│       ├── mcp-server.json         # MCP server monitoring
│       └── torus-monitoring.json   # Torus blockchain monitoring
└── README.md                       # This file
```

## Dashboards

### 1. Torus Monitoring Dashboard
- **File**: `torus-monitoring.json`
- **Purpose**: Monitor Torus blockchain metrics
- **Key Metrics**: Block height, block rate, network status

### 2. ChainSwarm Overview Dashboard
- **File**: `chainswarm-overview.json`
- **Purpose**: High-level system overview
- **Key Metrics**: System health, service status, resource utilization

### 3. API Monitoring Dashboard
- **File**: `api-monitoring.json`
- **Purpose**: Monitor API performance and health
- **Key Metrics**: Request rates, response times, error rates

### 4. Database Performance Dashboard
- **File**: `database-performance.json`
- **Purpose**: Monitor database metrics
- **Key Metrics**: Query performance, connection pools, storage usage

### 5. Indexer Performance Dashboard
- **File**: `indexer-performance.json`
- **Purpose**: Monitor indexer services
- **Key Metrics**: Processing rates, queue depths, error rates

### 6. MCP Server Dashboard
- **File**: `mcp-server.json`
- **Purpose**: Monitor MCP server performance
- **Key Metrics**: Connection status, request handling, resource usage

### 7. Logs Monitoring Dashboard
- **File**: `logs-monitoring.json`
- **Purpose**: Log aggregation and analysis
- **Key Metrics**: Log volumes, error patterns, service logs

## Configuration

### Datasources
- **Prometheus**: `http://infra-prometheus:9090`
- **Loki**: `http://infra-loki:3100`

### Environment Variables
Set the following environment variables in your `.env` file:
- `GRAFANA_ADMIN_USER`: Admin username (default: admin)
- `GRAFANA_ADMIN_PASSWORD`: Admin password (default: admin123)

## Access
- **URL**: http://localhost:3000
- **Default Credentials**: admin/admin123 (configurable via environment variables)

## Notes
- All dashboards are automatically provisioned on startup
- Datasources are configured to connect to the infrastructure services
- Dashboards are editable through the Grafana UI