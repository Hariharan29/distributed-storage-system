cmake_minimum_required(VERSION 3.16)
project(distributed_storage CXX)

enable_testing()

# ── C++ Standard ──────────────────────────────────────────────────────────────
# C++17 gives us std::filesystem, std::optional, and other modern features.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ── Find required packages ────────────────────────────────────────────────────

# gRPC and Protobuf: these find scripts locate the installed gRPC libraries.
find_package(gRPC CONFIG QUIET)
if (NOT gRPC_FOUND)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GRPC REQUIRED IMPORTED_TARGET grpc++)
    add_library(gRPC::grpc++ ALIAS PkgConfig::GRPC)
endif()
find_package(Protobuf REQUIRED)

# OpenSSL: needed by sha256.cpp
find_package(OpenSSL REQUIRED)

# SQLite3: needed by metadata_db.cpp
find_package(SQLite3 REQUIRED)

# ── Protobuf code generation ──────────────────────────────────────────────────
# This runs protoc and grpc_cpp_plugin to generate:
#   metadata.pb.h / metadata.pb.cc       (protobuf message classes)
#   metadata.grpc.pb.h / metadata.grpc.pb.cc  (gRPC service stubs)
#   storage.pb.h / storage.pb.cc
#   storage.grpc.pb.h / storage.grpc.pb.cc

# Locate the grpc_cpp_plugin executable.
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

# Directory where generated files will be placed.
set(PROTO_GEN_DIR "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

# ── Generate metadata.proto ───────────────────────────────────────────────────
set(METADATA_PROTO "${CMAKE_SOURCE_DIR}/proto/metadata.proto")

set(METADATA_PROTO_SRCS "${PROTO_GEN_DIR}/metadata.pb.cc")
set(METADATA_PROTO_HDRS "${PROTO_GEN_DIR}/metadata.pb.h")
set(METADATA_GRPC_SRCS  "${PROTO_GEN_DIR}/metadata.grpc.pb.cc")
set(METADATA_GRPC_HDRS  "${PROTO_GEN_DIR}/metadata.grpc.pb.h")

add_custom_command(
    OUTPUT  ${METADATA_PROTO_SRCS} ${METADATA_PROTO_HDRS}
            ${METADATA_GRPC_SRCS}  ${METADATA_GRPC_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            --proto_path "${CMAKE_SOURCE_DIR}/proto"
            --cpp_out    "${PROTO_GEN_DIR}"
            --grpc_out   "${PROTO_GEN_DIR}"
            --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXECUTABLE}"
            "${METADATA_PROTO}"
    DEPENDS ${METADATA_PROTO}
    COMMENT "Generating C++ from metadata.proto"
)

# ── Generate storage.proto ────────────────────────────────────────────────────
set(STORAGE_PROTO "${CMAKE_SOURCE_DIR}/proto/storage.proto")

set(STORAGE_PROTO_SRCS "${PROTO_GEN_DIR}/storage.pb.cc")
set(STORAGE_PROTO_HDRS "${PROTO_GEN_DIR}/storage.pb.h")
set(STORAGE_GRPC_SRCS  "${PROTO_GEN_DIR}/storage.grpc.pb.cc")
set(STORAGE_GRPC_HDRS  "${PROTO_GEN_DIR}/storage.grpc.pb.h")

add_custom_command(
    OUTPUT  ${STORAGE_PROTO_SRCS} ${STORAGE_PROTO_HDRS}
            ${STORAGE_GRPC_SRCS}  ${STORAGE_GRPC_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            --proto_path "${CMAKE_SOURCE_DIR}/proto"
            --cpp_out    "${PROTO_GEN_DIR}"
            --grpc_out   "${PROTO_GEN_DIR}"
            --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXECUTABLE}"
            "${STORAGE_PROTO}"
    DEPENDS ${STORAGE_PROTO}
    COMMENT "Generating C++ from storage.proto"
)

add_custom_target(generate_protos DEPENDS
    ${METADATA_PROTO_SRCS} ${METADATA_PROTO_HDRS}
    ${METADATA_GRPC_SRCS} ${METADATA_GRPC_HDRS}
    ${STORAGE_PROTO_SRCS} ${STORAGE_PROTO_HDRS}
    ${STORAGE_GRPC_SRCS} ${STORAGE_GRPC_HDRS}
)

# ── Shared proto library ──────────────────────────────────────────────────────
# All three executables (metadata_server, storage_node, client) link to this.
add_library(proto_lib
    ${METADATA_PROTO_SRCS}
    ${METADATA_GRPC_SRCS}
    ${STORAGE_PROTO_SRCS}
    ${STORAGE_GRPC_SRCS}
)
add_dependencies(proto_lib generate_protos)
target_include_directories(proto_lib PUBLIC ${PROTO_GEN_DIR})
target_link_libraries(proto_lib PUBLIC
    gRPC::grpc++
    protobuf::libprotobuf
)

# ── Shared common library ─────────────────────────────────────────────────────
add_library(common_lib
    common/sha256.cpp
)
target_include_directories(common_lib PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(common_lib PUBLIC OpenSSL::Crypto)

# ── metadata_server executable ────────────────────────────────────────────────
add_executable(metadata_server
    metadata_server/main.cpp
    metadata_server/metadata_db.cpp
    metadata_server/heartbeat_monitor.cpp
    metadata_server/replication_manager.cpp
    metadata_server/metadata_server.cpp
)
target_include_directories(metadata_server PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${PROTO_GEN_DIR}
)
target_link_libraries(metadata_server PRIVATE
    proto_lib
    common_lib
    SQLite::SQLite3
    gRPC::grpc++
)

# ── storage_node executable ───────────────────────────────────────────────────
add_executable(storage_node
    storage_node/main.cpp
    storage_node/chunk_store.cpp
    storage_node/heartbeat_sender.cpp
    storage_node/storage_node_server.cpp
)
target_include_directories(storage_node PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${PROTO_GEN_DIR}
)
target_link_libraries(storage_node PRIVATE
    proto_lib
    common_lib
    gRPC::grpc++
)

# ── client executable ─────────────────────────────────────────────────────────
add_executable(client
    client/main.cpp
    client/client.cpp
    client/chunker.cpp
)
target_include_directories(client PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${PROTO_GEN_DIR}
)
target_link_libraries(client PRIVATE
    proto_lib
    common_lib
    gRPC::grpc++
)

# ── Automated tests ──────────────────────────────────────────────────────────
# Tests link the production component implementations directly and use only
# temporary files and SQLite databases. Networked replication and full Docker
# workflows remain covered by the existing compose smoke tests.
add_executable(distributed_storage_tests
    tests/test_main.cpp
    client/chunker.cpp
    metadata_server/metadata_db.cpp
    storage_node/chunk_store.cpp
    storage_node/storage_node_server.cpp
)
target_include_directories(distributed_storage_tests PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${PROTO_GEN_DIR}
)
target_link_libraries(distributed_storage_tests PRIVATE
    proto_lib
    common_lib
    SQLite::SQLite3
    gRPC::grpc++
)
add_test(NAME distributed_storage_tests COMMAND distributed_storage_tests)
