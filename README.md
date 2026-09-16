# Fault-Tolerant Distributed Storage System

A fault-tolerant distributed file storage system built in C++17 using gRPC, Protobuf, SQLite, and Docker. Files are split into chunks, replicated across storage nodes, and automatically recovered when a storage node fails.

## Features

- **Chunked storage**: Files are split into 1 MiB chunks.
- **Replication**: Each chunk is normally stored on 2 storage nodes.
- **SHA-256 content addressing**: The SHA-256 hash of chunk contents is used as the chunk ID and filename on disk.
- **Deduplication**: Identical chunks are stored only once and can be referenced by multiple files.
- **Failure detection**: Storage nodes send heartbeats; nodes that stop responding are detected after the configured timeout.
- **Automatic re-replication**: Chunks affected by a failed node are copied to healthy nodes to restore the desired replication factor.
- **Adaptive replication**: Frequently accessed chunks can be promoted from 2 to 3 replicas.
- **Concurrent uploads**: Independent chunks are uploaded to storage nodes in parallel.
- **Metadata/data separation**: SQLite stores metadata while actual chunk data remains on storage nodes.
- **Dockerized deployment**: Docker Compose runs the metadata server and three storage nodes.

## Architecture

```text
                         ┌─────────────────────┐
                         │      CLI Client     │
                         └──────────┬──────────┘
                                    │
                             gRPC metadata
                                    │
                         ┌──────────▼──────────┐
                         │   Metadata Server   │
                         │      SQLite DB       │
                         │                     │
                         │ Heartbeat Monitor   │
                         │ Replication Manager │
                         └──────────┬──────────┘
                                    │
                              Control / Metadata
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
         ┌────▼─────┐          ┌────▼─────┐          ┌────▼─────┐
         │ Storage  │          │ Storage  │          │ Storage  │
         │  Node 1  │          │  Node 2  │          │  Node 3  │
         │  /data   │          │  /data   │          │  /data   │
         └──────────┘          └──────────┘          └──────────┘
              ▲                     ▲                     ▲
              └──────────── Direct chunk transfers ──────┘
                              from Client
```

### Component Responsibilities

| Component | Responsibility |
|---|---|
| **CLI Client** | File chunking, PUT/GET/DELETE/LIST, direct chunk transfers |
| **Metadata Server** | File/chunk metadata, replica locations, node state, upload coordination |
| **SQLite** | Persistent metadata storage |
| **Storage Nodes** | Store and retrieve chunk files |
| **Heartbeat Monitor** | Detect unavailable storage nodes |
| **Replication Manager** | Re-replicate chunks and promote hot chunks |
| **gRPC + Protobuf** | Communication between client, metadata server, and storage nodes |
| **Docker Compose** | Runs the complete distributed system locally |

## Design Highlights

### Metadata and data separation

The metadata server stores information such as:

- File records
- Chunk records
- File-to-chunk relationships
- Chunk-to-node locations
- Storage-node status
- Chunk access counts

Actual chunk contents are stored as files on the storage nodes.

### Content-addressed chunks

Each chunk is identified by:

```text
chunk_id = SHA-256(chunk data)
```

The same hash is also used as the chunk filename on the storage node.

This allows identical chunk contents to be recognized and reused.

### Replication

The default replication factor is 2. The metadata server selects healthy storage nodes for each chunk and tracks their locations in SQLite.

If a node fails, the replication manager restores missing replicas when healthy nodes are available.

## PUT Flow

```text
1. Client reads the input file
          ↓
2. File is split into 1 MiB chunks
          ↓
3. SHA-256 is calculated for each chunk
          ↓
4. Client sends chunk metadata to Metadata Server
          ↓
5. Metadata Server selects storage nodes
          ↓
6. Client uploads chunks directly to Storage Nodes
          ↓
7. Successful storage locations are recorded
          ↓
8. File metadata is finalized
```

Independent chunk uploads are performed concurrently.

## GET Flow

```text
1. Client requests file metadata
          ↓
2. Metadata Server returns ordered chunk information
          ↓
3. Client retrieves each chunk from a healthy replica
          ↓
4. Chunks are reassembled in index order
          ↓
5. Result is written to the requested output path
```

The client can try another replica if a storage node is unavailable.

## Fault Tolerance

### Node failure → re-replication

```text
             Before failure

        Node 1 ───── Chunk A
        Node 2 ───── Chunk A
        Node 3


             Node 1 fails
                  ↓

        Node 2 ───── Chunk A
        Node 3


          Heartbeat timeout
                  ↓
        Heartbeat Monitor
                  ↓
        Replication Manager
                  ↓
        Node 2 ─────────────► Node 3
                  CopyChunkTo
                  ↓

        Node 2 ───── Chunk A
        Node 3 ───── Chunk A
```

The recovery process is:

1. Storage node stops sending heartbeats.
2. After the configured timeout, `HeartbeatMonitor` marks the node as unavailable.
3. `ReplicationManager.handleNodeFailure()` identifies affected chunks.
4. Dead-node locations are removed from the metadata.
5. Under-replicated chunks are copied from a healthy source to a healthy target using `CopyChunkTo` gRPC.
6. The new replica location is recorded in SQLite.

The system therefore continues serving files when another healthy replica exists.

## Adaptive Replication

Frequently accessed chunks can receive an additional replica.

```text
Normal chunk:

Node 1 ── Chunk A
Node 2 ── Chunk A

             ↓
       Access count reaches
       configured hot threshold

             ↓

Node 1 ── Chunk A
Node 2 ── Chunk A
Node 3 ── Chunk A
```

The metadata server tracks chunk access information. The replication manager periodically checks for hot chunks and promotes eligible chunks to the configured hot replication factor.

## SHA-256 Deduplication

During a PUT:

1. The client calculates the SHA-256 hash of every chunk.
2. The metadata server checks whether that chunk already exists.
3. If the chunk already exists, the physical chunk does not need to be uploaded again.
4. The new file is associated with the existing chunk metadata.

For example:

```text
File A → Chunk X
File B → Chunk X
             │
             └── one physical copy of Chunk X per replica
```

This avoids storing identical chunk contents multiple times.

## Testing

The repository includes an automated C++17 test suite in `tests/test_main.cpp`,
integrated with CMake and CTest. It currently contains 19 test cases covering:

- SHA-256 known vectors, deterministic hashing, binary data, and file hashing
- Chunking boundaries, ordering, hashing, and multi-chunk reassembly
- ChunkStore filesystem write, read, deduplication, and delete behavior
- SQLite metadata schema and file/chunk round trips
- Deduplication and shared chunk references
- Replication-related metadata, including unique locations and healthy-node filtering
- Access counts, hot-chunk eligibility, node status, and heartbeat metadata
- Legacy SQLite schema migration
- Concurrent metadata operations
- Storage-node hash validation and chunk RPC behavior

Run the automated suite with:

```bash
cmake -S . -B build-tests -DCMAKE_BUILD_TYPE=Debug
cmake --build build-tests
ctest --test-dir build-tests --output-on-failure
```

The current verified result is 19 test cases with 0 failures.

Full distributed replication, node-failure recovery, automatic re-replication,
adaptive/hot-chunk replication, and end-to-end PUT/GET/DELETE/LIST behavior are
validated through the Docker-based system workflow rather than duplicated as
unit tests. The Docker workflow also covers persistence across container
restarts and retrieval after a storage-node failure.

### Node Failure Test

A storage node containing replicas of `bigfile.bin` was stopped during testing.

The remaining storage node successfully served all 5 logical chunks:

```text
[Client] File has 5 chunk(s).
[Client] Chunk 0 fetched from storage_node2:50051
[Client] Chunk 1 fetched from storage_node2:50051
[Client] Chunk 2 fetched from storage_node2:50051
[Client] Chunk 3 fetched from storage_node2:50051
[Client] Chunk 4 fetched from storage_node2:50051
[Client] File saved to: /uploads/failure_test.bin
```

The reconstructed file was compared against the original using a binary comparison:

```text
FC: no differences encountered
```

## Prerequisites

### Docker

- Docker Desktop with Docker Compose

### Local build

- CMake 3.16+
- C++17 compiler
- gRPC C++
- Protobuf
- OpenSSL
- SQLite3

## Quick Start with Docker

```bash
# 1. Clone the repository and enter the project directory
git clone <repository-url>
cd distributed_storage

# 2. Build and start all services
#    metadata server + 3 storage nodes
docker compose up --build
```

Keep the Compose process running. Open a **new terminal** for client commands.

Put files you want to upload in the project's `uploads/` directory. The directory is mounted into the client container as `/uploads`.

```bash
# Upload a file
docker compose run --rm client put /uploads/myfile.pdf

# List stored files
docker compose run --rm client list

# Download a file
docker compose run --rm client get myfile.pdf /uploads/downloaded.pdf

# Delete a file
docker compose run --rm client delete myfile.pdf
```

Stop the services:

```bash
docker compose down
```

Stop the services and remove their Docker volumes, including stored metadata and chunk data:

```bash
docker compose down -v
```

> **Warning:** `docker compose down -v` deletes the persistent Docker volumes used by the metadata server and storage nodes.

## Local Build Without Docker

### Install dependencies on Ubuntu/Debian

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

```text
build/metadata_server
build/storage_node
build/client
```

### Run locally

Run the metadata server first:

```bash
DB_PATH=./metadata.db ./build/metadata_server
```

Run three storage nodes in separate terminals:

```bash
NODE_ID=node1 NODE_PORT=50051 METADATA_ADDR=localhost:50050 \
    DATA_DIR=./data/node1 HOSTNAME=localhost ./build/storage_node
```

```bash
NODE_ID=node2 NODE_PORT=50052 METADATA_ADDR=localhost:50050 \
    DATA_DIR=./data/node2 HOSTNAME=localhost ./build/storage_node
```

```bash
NODE_ID=node3 NODE_PORT=50053 METADATA_ADDR=localhost:50050 \
    DATA_DIR=./data/node3 HOSTNAME=localhost ./build/storage_node
```

Run the client:

```bash
METADATA_ADDR=localhost:50050 ./build/client list
METADATA_ADDR=localhost:50050 ./build/client put ./myfile.pdf
METADATA_ADDR=localhost:50050 ./build/client get myfile.pdf ./out.pdf
METADATA_ADDR=localhost:50050 ./build/client delete myfile.pdf
```

> **Note:** When running multiple storage nodes locally, each node must use a different `NODE_PORT`. Set `HOSTNAME=localhost` so nodes register reachable local addresses with the metadata server.

## Configuration

All configuration constants are defined in [`common/config.h`](common/config.h).

| Constant | Default | Description |
|---|---:|---|
| `CHUNK_SIZE_BYTES` | 1 MiB | File chunk size |
| `DEFAULT_REPLICATION_FACTOR` | 2 | Normal replicas per chunk |
| `HOT_REPLICATION_FACTOR` | 3 | Replicas for hot chunks |
| `HOT_CHUNK_THRESHOLD` | 10 | Access threshold for hot chunks |
| `HEARTBEAT_INTERVAL_SECONDS` | 5 | Node heartbeat interval |
| `HEARTBEAT_TIMEOUT_SECONDS` | 15 | Failure detection timeout |
| `ADAPTIVE_REPLICATION_INTERVAL_SECONDS` | 60 | Hot-chunk evaluation interval |

Change the values in `common/config.h` and rebuild.

## Project Structure

```text
distributed_storage/
├── proto/
│   ├── metadata.proto          ← Client ↔ Metadata Server
│   └── storage.proto           ← Storage Node gRPC interface
├── common/
│   ├── config.h                ← Configuration constants
│   └── sha256.h / .cpp         ← SHA-256 utility
├── metadata_server/
│   ├── metadata_db.h/.cpp
│   ├── heartbeat_monitor.h/.cpp
│   ├── replication_manager.h/.cpp
│   ├── metadata_server.h/.cpp
│   └── main.cpp
├── storage_node/
│   ├── chunk_store.h/.cpp
│   ├── heartbeat_sender.h/.cpp
│   ├── storage_node_server.h/.cpp
│   └── main.cpp
├── client/
│   ├── chunker.h/.cpp
│   ├── client.h/.cpp
│   └── main.cpp
├── docker/
│   ├── Dockerfile.metadata
│   ├── Dockerfile.storage
│   └── Dockerfile.client
├── docker-compose.yml
└── CMakeLists.txt
```

## Technology Stack

- **C++17** — core implementation
- **gRPC** — service-to-service communication
- **Protocol Buffers** — RPC message definitions
- **SQLite** — persistent metadata
- **OpenSSL** — SHA-256 hashing
- **Docker / Docker Compose** — containerized deployment
- **CMake** — build system
- **POSIX filesystem APIs / C++ filesystem** — chunk storage

## Limitations

This project intentionally focuses on core distributed-storage concepts and does not implement:

- Consensus-based metadata replication such as Raft
- Erasure coding
- Distributed metadata across multiple metadata servers
- Object-storage APIs such as S3
- FUSE filesystem mounting
- Kubernetes orchestration

The current architecture uses a single metadata server, while storage data is replicated across three storage nodes.

## Future Improvements

Possible extensions include:

- Metadata-server replication
- Stronger consistency guarantees
- More sophisticated replica placement
- Background integrity verification
- Configurable retry policies
- Performance benchmarking under larger workloads
- Additional storage-node failure scenarios

## License

This project is intended as an educational distributed-systems project.
