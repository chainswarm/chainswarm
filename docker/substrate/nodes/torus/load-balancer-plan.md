# Load Balancer Setup for Multiple Torus Nodes

This document outlines the plan for setting up multiple Torus nodes with an Nginx load balancer to provide a single RPC endpoint.

## Overview

We'll create a Docker Compose setup that:
1. Runs multiple Torus node instances (2 nodes initially)
2. Sets up an Nginx load balancer to distribute traffic between these nodes
3. Exposes a single RPC endpoint via the load balancer

## File Structure

We'll need to create two files:
1. `lb-docker-compose.yml` - Docker Compose configuration for the nodes and load balancer
2. `nginx.conf` - Nginx configuration for load balancing

## Docker Compose Configuration

The `lb-docker-compose.yml` file will contain:

```yaml
version: '3.8'

services:
  infra-torus-node1:
    container_name: infra-torus-node1
    image: ghcr.io/renlabs-dev/torus-substrate:b1402c9
    restart: unless-stopped
    ports:
      - "30333:30333"  # P2P Port (Node 1)
      - "9615:9615"    # Prometheus (Node 1)
    command:
      - torus-node
      - --base-path=/chain-data
      - --keystore-path=/keystore
      - --chain=/chain-data/specs/chainspec.json
      - --prometheus-external
      - --prometheus-port=9615
      - --unsafe-rpc-external
      - --rpc-port=9944
      - --rpc-cors=all
      - --rpc-methods=unsafe
      - --in-peers=64
      - --rpc-max-connections=100000
      - --rpc-max-response-size=20480
      - --public-addr=/dns4/infra-torus-node1/tcp/30333
      - --listen-addr=/ip4/0.0.0.0/tcp/30333
      - --pruning=archive
    volumes:
      - ./torus-specs-mainnet:/chain-data/specs
      - infra-torus-chain-data-node1:/chain-data
      - infra-torus-keystore-node1:/keystore
    networks:
      - torus-network

  infra-torus-node2:
    container_name: infra-torus-node2
    image: ghcr.io/renlabs-dev/torus-substrate:b1402c9
    restart: unless-stopped
    ports:
      - "30334:30333"  # P2P Port (Node 2)
      - "9616:9615"    # Prometheus (Node 2)
    command:
      - torus-node
      - --base-path=/chain-data
      - --keystore-path=/keystore
      - --chain=/chain-data/specs/chainspec.json
      - --prometheus-external
      - --prometheus-port=9615
      - --unsafe-rpc-external
      - --rpc-port=9944
      - --rpc-cors=all
      - --rpc-methods=unsafe
      - --in-peers=64
      - --rpc-max-connections=100000
      - --rpc-max-response-size=20480
      - --public-addr=/dns4/infra-torus-node2/tcp/30333
      - --listen-addr=/ip4/0.0.0.0/tcp/30333
      - --pruning=archive
    volumes:
      - ./torus-specs-mainnet:/chain-data/specs
      - infra-torus-chain-data-node2:/chain-data
      - infra-torus-keystore-node2:/keystore
    networks:
      - torus-network

  nginx-lb:
    image: nginx:latest
    container_name: torus-rpc-lb
    ports:
      - "9944:80"  # Expose load balancer on host port 9944
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
    networks:
      - torus-network
    depends_on:
      - infra-torus-node1
      - infra-torus-node2

volumes:
  infra-torus-chain-data-node1:
  infra-torus-keystore-node1:
  infra-torus-chain-data-node2:
  infra-torus-keystore-node2:

networks:
  torus-network:
    driver: bridge
```

## Nginx Configuration

The `nginx.conf` file will contain:

```nginx
events {
    worker_connections 1024;
}

http {
    upstream torus_nodes {
        # Use Docker service names to target containers
        server infra-torus-node1:9944;
        server infra-torus-node2:9944;
        # Add more nodes here as needed
    }

    server {
        listen 80;

        location / {
            proxy_pass http://torus_nodes;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            # Timeout settings for WebSocket
            proxy_read_timeout 86400;
        }
    }
}
```

## Key Features

1. **Multiple Node Configuration**:
   - Each node uses distinct volumes to avoid data conflicts
   - Host ports for P2P (30333, 30334) and Prometheus (9615, 9616) are unique to avoid collisions
   - Both nodes share the same chainspec files

2. **Load Balancer**:
   - Nginx listens on host port 9944 and distributes traffic to the RPC ports of all Torus nodes
   - WebSocket support is included for Substrate RPC compatibility
   - Simple round-robin load balancing is used by default

3. **Network Isolation**:
   - All services are on the same Docker network (torus-network), allowing internal communication using service names

4. **Node Discovery**:
   - The `--public-addr` flag uses Docker service names to ensure nodes can discover each other internally

## Implementation Steps

1. Create the `lb-docker-compose.yml` file in the `docker/substrate/nodes/torus/` directory
2. Create the `nginx.conf` file in the same directory
3. Start the services with `docker-compose -f lb-docker-compose.yml up -d`

## Scaling

To add more nodes:
1. Add additional node services to the Docker Compose file with unique names, ports, and volumes
2. Add the new node services to the upstream section in the Nginx configuration
3. Restart the services

## Monitoring

- Each node exposes its Prometheus metrics on a unique port (9615, 9616)
- The load balancer distributes RPC traffic, but each node can be monitored individually

## Maintenance

- Individual nodes can be restarted without affecting the entire system
- The load balancer will automatically route traffic to available nodes