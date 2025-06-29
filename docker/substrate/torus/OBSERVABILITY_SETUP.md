# Torus Infrastructure Observability Setup

This document describes the complete observability stack setup for the Torus infrastructure, including Prometheus, Grafana, Loki, and Promtail.

## Overview

The observability stack consists of:
- **Prometheus**: Metrics collection and storage
- **Grafana**: Visualization and dashboards
- **Loki**: Log aggregation and storage
- **Promtail**: Log collection agent

## Services Added to infra-docker-compose.yml

### 1. Grafana (`infra-grafana`)
- **Image**: `grafana/grafana:latest`
- **Port**: `3000:3000`
- **Features**: 
  - Auto-provisioned datasources (Prometheus & Loki)
  - Pre-configured dashboards
  - Admin credentials via environment variables

### 2. Loki (`infra-loki`)
- **Image**: `grafana/loki:2.9.0`
- **Port**: `3100:3100`
- **Features**:
  - Filesystem-based storage
  - BoltDB shipper for indexing
  - Query result caching

### 3. Promtail (`infra-promtail`)
- **Image**: `grafana/promtail:2.9.0`
- **Features**:
  - Docker container log collection
  - System log collection
  - ChainSwarm application log collection

## Directory Structure

```
docker/substrate/torus/
├── infra-docker-compose.yml        # Updated with observability services
├── .env.grafana.example            # Environment variables template
├── OBSERVABILITY_SETUP.md          # This file
├── grafana/
│   ├── README.md
│   └── provisioning/
│       ├── datasources/
│       │   └── datasources.yml     # Prometheus & Loki datasources
│       └── dashboards/
│           ├── dashboard.yml       # Dashboard provider config
│           ├── api-monitoring.json
│           ├── chainswarm-overview.json
│           ├── database-performance.json
│           ├── indexer-performance.json
│           ├── logs-monitoring.json
│           ├── mcp-server.json
│           └── torus-monitoring.json
├── loki/
│   ├── README.md
│   └── local-config.yaml          # Loki configuration
└── promtail/
    ├── README.md
    └── config.yml                 # Promtail configuration
```

## Quick Start

### 1. Environment Setup
```bash
# Copy environment template
cp docker/substrate/torus/.env.grafana.example docker/substrate/torus/.env

# Edit environment variables as needed
# GRAFANA_ADMIN_USER=admin
# GRAFANA_ADMIN_PASSWORD=admin123
```

### 2. Start Services
```bash
# Navigate to the directory
cd docker/substrate/torus

# Start the observability stack
docker-compose -f infra-docker-compose.yml up -d infra-prometheus infra-grafana infra-loki infra-promtail
```

### 3. Access Services
- **Grafana**: http://localhost:3000 (admin/admin123)
- **Prometheus**: http://localhost:9090
- **Loki**: http://localhost:3100

## Available Dashboards

1. **Torus Monitoring**: Blockchain metrics and block processing
2. **ChainSwarm Overview**: High-level system health and status
3. **API Monitoring**: API performance, response times, error rates
4. **Database Performance**: Database metrics and query performance
5. **Indexer Performance**: Indexer service monitoring and processing rates
6. **MCP Server**: MCP server performance and connection status
7. **Logs Monitoring**: Log aggregation, error patterns, and analysis

## Network Integration

All services are connected to the existing `torus-network` external network, ensuring seamless communication with other infrastructure components.

## Volume Management

Persistent volumes are created for:
- `infra-grafana-data`: Grafana configuration and dashboards
- `infra-loki-data`: Loki log storage
- `infra-prometheus-data`: Prometheus metrics storage (existing)

## Monitoring Targets

### Prometheus Targets
- Infrastructure services (ClickHouse, Memgraph, Neo4j)
- Application services (APIs, indexers)
- System metrics

### Log Sources (Promtail)
- Docker container logs
- System logs (`/var/log/syslog`)
- ChainSwarm application logs (`/var/log/chainswarm/*.log`)

## Security Considerations

- Default Grafana credentials should be changed in production
- Consider enabling HTTPS for external access
- Implement proper authentication for production deployments
- Review log retention policies for compliance

## Troubleshooting

### Common Issues
1. **Services not starting**: Check network connectivity and port conflicts
2. **No metrics in Grafana**: Verify Prometheus targets are up
3. **No logs in Loki**: Check Promtail configuration and file permissions
4. **Dashboard not loading**: Verify datasource configuration

### Health Checks
```bash
# Check service status
docker-compose -f infra-docker-compose.yml ps

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Check Loki readiness
curl http://localhost:3100/ready

# Check Grafana health
curl http://localhost:3000/api/health
```

## Next Steps

1. Configure alerting rules in Prometheus
2. Set up notification channels in Grafana
3. Implement log-based alerts in Loki
4. Add custom metrics to applications
5. Configure log rotation and retention policies

## Support

For issues or questions:
1. Check service logs: `docker-compose -f infra-docker-compose.yml logs [service-name]`
2. Review configuration files in respective directories
3. Consult individual README files for detailed configuration options