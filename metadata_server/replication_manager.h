#pragma once

// =============================================================================
// replication_manager.h — Handles re-replication and adaptive replication
//
// Python analogy: think of this as a "repair worker" class.
// It runs jobs when called by the HeartbeatMonitor, not continuously by itself.
//
// Two responsibilities:
//   1. handleNodeFailure()   → re-replicate chunks that lost a replica
//   2. promoteHotChunks()    → add extra replicas to frequently accessed chunks
//
// To copy a chunk from node A to node B, we call gRPC CopyChunkTo() on node A.
// Node A reads the chunk from its disk and pushes it to node B directly.
// =============================================================================

#include "metadata_db.h"

#include <string>
#include <memory>

class ReplicationManager {
public:
    // db is a reference (not a copy) — like passing an object by reference in Python.
    explicit ReplicationManager(MetadataDB& db);

    // Called by HeartbeatMonitor when a node is detected as dead.
    // Finds all chunks that now have fewer replicas than required and repairs them.
    void handleNodeFailure(const std::string& failed_node_id);

    // Called periodically by HeartbeatMonitor to implement adaptive replication.
    // Promotes "hot" chunks (access_count >= threshold) to a higher replica count.
    void promoteHotChunks();

private:
    MetadataDB& db_;  // Reference to the shared database (not owned here)

    // Internal helper: ask source_node to copy chunk_id to target_node.
    // Returns true if the copy succeeded.
    bool replicateChunk(const std::string& chunk_id,
                        const std::string& source_node_address,
                        const std::string& target_node_address);

    // Pick a healthy node that does NOT already hold chunk_id.
    // Returns empty string if no such node exists.
    std::string pickTargetNode(const std::string& chunk_id);
};
