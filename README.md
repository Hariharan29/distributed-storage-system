# Fault-Tolerant Distributed Storage System

A small educational distributed file storage system written in C++.

## Features

- **Chunked storage**: Files are split into 1MB chunks.
- **Replication**: Each chunk is stored on 2 storage nodes (default).
- **SHA-256 deduplication**: Identical chunks are stored only once.
- **Failure detection**: Heartbeat-based; dead nodes detected in ~15 seconds.
- **Re-replication**: Chunks on dead nodes are automatically repaired.
- **Adaptive replication**: Hot chunks (≥10 accesses) are promoted to 3 replicas.
- **Concurrent uploads**: Chunks are uploaded to nodes in parallel.

## Architecture

```
Client ──► Metadata Server (SQLite)
  │              │
  │         Heartbeat Monitor
  │         Replication Manager
  │
  └──► Storage Node 1  (/data)
  └──► Storage Node 2  (/data)
  └──► Storage Node 3  (/data)
```

## Prerequisites

- Docker Desktop (for Docker build)
- OR: CMake 3.16+, g++, gRPC, OpenSSL, SQLite3 (for local build)

---

## Quick Start with Docker

```bash
# 1. Clone / enter the project directory
cd distributed_storage

# 2. Build and start all services (metadata server + 3 storage nodes)
docker compose up --build

# 3. In a new terminal — upload a file
docker compose run --rm client put /uploads/myfile.pdf

# 4. List all stored files
docker compose run --rm client list

# 5. Download a file
docker compose run --rm client get myfile.pdf /uploads/downloaded.pdf

# 6. Delete a file
docker compose run --rm client delete myfile.pdf

# 7. Stop everything
docker compose down

# 8. Stop everything AND delete all stored data
docker compose down -v
```

> **Tip:** Put files you want to upload in the `./uploads/` directory —
> it is mounted into the client container at `/uploads/`.

---

## Local Build (without Docker)

### Install dependencies (Ubuntu/Debian)

```bash
sudo apt-get install -y \
    cmake g++ \
    libgrpc++-dev libprotobuf-dev \
    protobuf-compiler protobuf-compiler-grpc \
    libsqlite3-dev libssl-dev
```

### Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -- -j$(nproc)
```

Binaries are created at:
- `build/metadata_server`
- `build/storage_node`
- `build/client`

### Run locally (4 terminals)

**Terminal 1 — Metadata server:**
```bash
DB_PATH=./metadata.db ./build/metadata_server
```

**Terminal 2 — Storage node 1:**
```bash
NODE_ID=node1 NODE_PORT=50051 METADATA_ADDR=localhost:50050 \
    DATA_DIR=./data/node1 HOSTNAME=localhost ./build/storage_node
```

**Terminal 3 — Storage node 2:**
```bash
NODE_ID=node2 NODE_PORT=50052 METADATA_ADDR=localhost:50050 \
    DATA_DIR=./data/node2 HOSTNAME=localhost ./build/storage_node
```

**Terminal 4 — Client:**
```bash
METADATA_ADDR=localhost:50050 ./build/client list
METADATA_ADDR=localhost:50050 ./build/client put ./myfile.pdf
METADATA_ADDR=localhost:50050 ./build/client get myfile.pdf ./out.pdf
```

> **Note for local multi-node runs:** When running multiple nodes locally
> (not in Docker), each node must listen on a different port. Set
> `HOSTNAME=localhost` and use different `NODE_PORT` values. The node
> registers its own address with the metadata server using `hostname:port`.

---

## Configuration

All constants are in [`common/config.h`](common/config.h).
Change values there and rebuild — no other file needs to change.

| Constant | Default | Description |
|---|---|---|
| `CHUNK_SIZE_BYTES` | 1 MB | File chunk size |
| `DEFAULT_REPLICATION_FACTOR` | 2 | Normal replicas per chunk |
| `HOT_REPLICATION_FACTOR` | 3 | Replicas for hot chunks |
| `HOT_CHUNK_THRESHOLD` | 10 | Accesses to become "hot" |
| `HEARTBEAT_INTERVAL_SECONDS` | 5 | Node ping interval |
| `HEARTBEAT_TIMEOUT_SECONDS` | 15 | Time before node is marked dead |
| `ADAPTIVE_REPLICATION_INTERVAL_SECONDS` | 60 | How often to promote hot chunks |

---

## Project Structure

```
distributed_storage/
├── proto/                  ← gRPC/Protobuf definitions
│   ├── metadata.proto      ← Client ↔ Metadata Server
│   └── storage.proto       ← Metadata Server ↔ Storage Nodes
├── common/
│   ├── config.h            ← All constants
│   ├── sha256.h / .cpp     ← SHA-256 utility
├── metadata_server/
│   ├── metadata_db.h/.cpp          ← SQLite wrapper
│   ├── heartbeat_monitor.h/.cpp    ← Dead node detection
│   ├── replication_manager.h/.cpp  ← Re-replication + hot promotion
│   ├── metadata_server.h/.cpp      ← gRPC service
│   └── main.cpp
├── storage_node/
│   ├── chunk_store.h/.cpp          ← Filesystem I/O
│   ├── heartbeat_sender.h/.cpp     ← Sends pings to metadata server
│   ├── storage_node_server.h/.cpp  ← gRPC service
│   └── main.cpp
├── client/
│   ├── chunker.h/.cpp      ← File splitting
│   ├── client.h/.cpp       ← PUT/GET/DELETE/LIST
│   └── main.cpp            ← CLI
├── docker/
│   ├── Dockerfile.metadata
│   ├── Dockerfile.storage
│   └── Dockerfile.client
├── docker-compose.yml
└── CMakeLists.txt
```

---

## How Fault Tolerance Works

### Node failure → re-replication

1. Storage node stops sending heartbeats.
2. After 15 seconds, `HeartbeatMonitor` detects the timeout.
3. `ReplicationManager.handleNodeFailure()` is called.
4. All chunks that were on the dead node are identified.
5. For each under-replicated chunk: a healthy source node is instructed
   to copy the chunk to a healthy target node via `CopyChunkTo` gRPC.
6. The `chunk_locations` table is updated.

### Adaptive replication (hot chunks)

1. Every `GET`, the metadata server increments `access_count` for each chunk.
2. Every 60 seconds, `ReplicationManager.promoteHotChunks()` runs.
3. Any chunk with `access_count >= 10` gets promoted to 3 replicas.
4. The extra replica is added via the same `CopyChunkTo` mechanism.

---

## SHA-256 Deduplication

- `chunk_id = SHA-256(chunk data)` — the chunk's name on disk IS its hash.
- During `PUT`, the client sends SHA-256 hashes to the metadata server.
- If a hash already exists in the `chunks` table, no upload happens for
  that chunk — the metadata server just records the new file's association
  with the existing chunk data.
