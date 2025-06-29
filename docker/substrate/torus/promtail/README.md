# Promtail Configuration for Torus Infrastructure

This directory contains the Promtail configuration for log collection in the Torus infrastructure.

## Configuration File

### config.yml
This is the main Promtail configuration file that defines log collection jobs and processing pipelines.

## Log Collection Jobs

### 1. Container Logs (`containers`)
- **Source**: `/var/lib/docker/containers/*/*log`
- **Purpose**: Collect logs from all Docker containers
- **Labels**: `job=containerlogs`, `container_name`, `stream`
- **Processing**: JSON parsing to extract log content and metadata

### 2. System Logs (`syslog`)
- **Source**: `/var/log/syslog`
- **Purpose**: Collect system-level logs
- **Labels**: `job=syslog`

### 3. ChainSwarm Application Logs (`chainswarm_logs`)
- **Source**: `/var/log/chainswarm/*.log`
- **Purpose**: Collect application-specific logs
- **Labels**: `job=chainswarm`, `level`
- **Processing**: Regex parsing to extract timestamp, log level, and message

## Pipeline Stages

### Container Logs Pipeline
1. **JSON Parsing**: Extract `log`, `stream`, and `attrs` from Docker log format
2. **Tag Extraction**: Parse container name from log attributes
3. **Timestamp Processing**: Parse RFC3339Nano timestamps
4. **Label Assignment**: Add `stream` and `container_name` labels
5. **Output Formatting**: Clean log output

### ChainSwarm Logs Pipeline
1. **Pattern Matching**: Match logs with `job="chainswarm"`
2. **Regex Parsing**: Extract timestamp, log level, and message
3. **Timestamp Processing**: Parse custom timestamp format
4. **Label Assignment**: Add `level` label for log filtering

## Configuration Details

### Server Settings
- **HTTP Port**: 9080 (metrics and health checks)
- **gRPC Port**: 0 (disabled)

### Client Configuration
- **Loki URL**: `http://infra-loki:3100/loki/api/v1/push`
- **Position File**: `/tmp/positions.yaml` (tracks read positions)

### Volume Mounts Required
- `/var/log:/var/log:ro` - System logs (read-only)
- `/var/lib/docker/containers:/var/lib/docker/containers:ro` - Container logs (read-only)
- `/var/run/docker.sock:/var/run/docker.sock` - Docker socket for metadata

## Log Labels

### Automatic Labels
- `job`: Identifies the log source type
- `container_name`: Docker container name (for container logs)
- `stream`: stdout/stderr (for container logs)
- `level`: Log level (for chainswarm logs)

### Custom Labels
You can add custom labels by modifying the `static_configs` section in each job.

## Monitoring

Promtail exposes metrics on port 9080:
- `http://infra-promtail:9080/metrics`

Key metrics include:
- `promtail_read_bytes_total`: Bytes read from log files
- `promtail_sent_entries_total`: Log entries sent to Loki
- `promtail_dropped_entries_total`: Dropped log entries

## LogQL Query Examples

```logql
# All container logs
{job="containerlogs"}

# Logs from specific container
{job="containerlogs", container_name="infra-prometheus"}

# Error logs from all sources
{job="containerlogs"} |= "ERROR"

# ChainSwarm application logs by level
{job="chainswarm", level="ERROR"}

# System logs
{job="syslog"}
```

## Troubleshooting

### Common Issues
1. **Permission Denied**: Ensure Promtail has read access to log directories
2. **No Logs Appearing**: Check file paths and Docker socket access
3. **High Memory Usage**: Adjust batch size and processing limits

### Debug Mode
Add to config.yml for debugging:
```yaml
server:
  log_level: debug
```

### Position File
The position file tracks where Promtail left off reading logs. Delete `/tmp/positions.yaml` to re-read all logs from the beginning.