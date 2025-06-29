# Loki Configuration for Torus Infrastructure

This directory contains the Loki configuration for log aggregation in the Torus infrastructure.

## Configuration File

### local-config.yaml
This is the main Loki configuration file that defines:

- **Server Settings**: HTTP and gRPC listen ports
- **Storage**: Filesystem-based storage for chunks and rules
- **Schema**: BoltDB shipper configuration for index storage
- **Query Range**: Caching configuration for improved performance

## Key Features

### Storage Configuration
- **Chunks Directory**: `/loki/chunks` - Where log chunks are stored
- **Rules Directory**: `/loki/rules` - Where alerting rules are stored
- **Index Storage**: BoltDB shipper with filesystem object store

### Performance Settings
- **Results Cache**: 100MB embedded cache for query results
- **Replication Factor**: 1 (single instance setup)
- **Schema Version**: v11 (latest stable schema)

### Network Configuration
- **HTTP Port**: 3100 (accessible via `http://infra-loki:3100`)
- **gRPC Port**: 9096 (internal communication)

## Integration

### With Promtail
Promtail is configured to send logs to Loki at `http://infra-loki:3100/loki/api/v1/push`

### With Grafana
Grafana is configured with Loki as a datasource at `http://infra-loki:3100`

## Log Retention

The current configuration uses filesystem storage with no explicit retention policy. 
For production use, consider:
- Adding retention policies
- Configuring object storage (S3, GCS, etc.)
- Setting up compaction schedules

## Monitoring

Loki exposes metrics on the HTTP port that can be scraped by Prometheus for monitoring:
- `http://infra-loki:3100/metrics`

## Usage

Once running, you can:
1. Query logs directly via HTTP API
2. Use Grafana's Explore feature with LogQL queries
3. Create log-based alerts and dashboards
4. Stream logs in real-time

## LogQL Examples

```logql
# All logs from a specific container
{container_name="infra-prometheus"}

# Error logs across all containers
{job="containerlogs"} |= "ERROR"

# Logs from chainswarm services
{job="chainswarm"}

# Rate of error logs per minute
rate({job="containerlogs"} |= "ERROR"[1m])