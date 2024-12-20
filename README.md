# Distributed Key-Value Storage with Disk Snapshots

An efficient distributed in-memory key-value store featuring disk snapshots for data persistence, horizontal scaling capabilities, and fault tolerance through peer replication. The system employs a broker-based architecture for request routing and dynamic load balancing.

## Core Features

- **In-Memory Storage**: Fast read/write operations with in-memory data storage
- **Data Persistence**: Automatic periodic snapshots and manual snapshot capability
- **Fault Tolerance**: Dual-site data replication with peer recovery mechanisms
- **Dynamic Scaling**: Runtime addition of new key-value store nodes
- **Load Balancing**: Intelligent request routing based on node load metrics
- **Zero Downtime**: Maintained data accessibility during node failures

## System Architecture

### Broker Server
- Manages request routing and node registration
- Tracks node loads for optimal distribution
- Coordinates data replication across nodes
- Handles system-wide snapshots

### Key-Value Store Nodes
- Maintains in-memory data storage
- Implements peer replication
- Manages local and peer snapshots
- Supports automatic recovery mechanisms

## API Endpoints

### Broker Endpoints
- `POST /set`: Store a key-value pair
- `GET /get`: Retrieve a value by key
- `GET /getall`: List all stored key-value pairs
- `POST /kvstore/snapshot/manual`: Trigger manual snapshot
- `GET /stores/list`: List all active store nodes
- `DELETE /delete`: Remove a key-value pair
- `POST /register`: Register new key-value store nodes

## Setup Instructions

### Prerequisites
- Go (latest stable version)
- Network connectivity between nodes
- Read/write permissions for snapshot directories

### Starting the System

1. **Launch the Broker**:
```bash
go run servermain/main.go
```

2. **Set Broker URL Environment Variable**:
```bash
# For Mac/Linux
export BROKER_URL="http://localhost:8080/register"

# For Windows
$env:BROKER_URL="http://localhost:8080/register"
```

3. **Start Key-Value Store Nodes**:
```bash
go run kvstoremain/kvstore_server.go store1 8081
```

## Usage Examples

### Store a Key-Value Pair
```bash
curl -X POST "http://localhost:8080/set" \
  -H "Content-Type: application/json" \
  -d '{
    "key": "k1",
    "value": "v1"
  }'
```

### Retrieve a Value
```bash
curl "http://localhost:8080/get?key=k2"
```

### List All Keys
```bash
curl "http://localhost:8080/getall"
```

### Trigger Manual Snapshot
```bash
curl -X POST "http://localhost:8080/kvstore/snapshot/manual"
```

## Fault Tolerance

The system implements robust fault tolerance through:
- Dual-site data replication
- Automatic peer recovery mechanisms
- Periodic snapshot creation
- Manual snapshot capability

When a node fails:
1. Broker detects the failure
2. Peer node initiates recovery process
3. Data is restored from peer snapshots
4. System continues operation with zero downtime

## Scaling

The system supports horizontal scaling by:
- Adding new nodes during runtime
- Automatically distributing load to new nodes
- Maintaining replication across scaled infrastructure
- Preserving data consistency during scaling operations

## Load Balancing

Load balancing is achieved through:
- Dynamic load monitoring of all nodes
- Intelligent distribution of new keys
- Routing requests to least loaded nodes
- Continuous load optimization

## Data Persistence

Data durability is ensured through:
- Automatic periodic snapshots
- Manual snapshot capability
- Dual-site replication
- Peer backup mechanisms