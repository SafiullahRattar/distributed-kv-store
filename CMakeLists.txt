cmake_minimum_required(VERSION 3.16)
project(distributed_kv_store
  VERSION   1.0.0
  LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(BUILD_TESTS      "Build unit tests"  ON)
option(BUILD_BENCHMARKS "Build benchmarks"  ON)

# ---------------------------------------------------------------------------
# Dependencies via find_package
# ---------------------------------------------------------------------------
find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC     CONFIG QUIET)

# If gRPC was not found via CONFIG, fall back to pkg-config / manual
if(NOT gRPC_FOUND)
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)
  find_library(GRPC_LIB       grpc)
  find_library(GRPCPP_LIB     grpc++)
  if(NOT GRPC_CPP_PLUGIN OR NOT GRPC_LIB OR NOT GRPCPP_LIB)
    message(FATAL_ERROR "gRPC not found. Install grpc and grpc++ development packages.")
  endif()
  message(STATUS "Using system gRPC via manual detection")
  set(GRPC_LIBRARIES ${GRPCPP_LIB} ${GRPC_LIB})
else()
  message(STATUS "Using gRPC via CMake CONFIG")
  set(GRPC_LIBRARIES gRPC::grpc++)
  get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin LOCATION)
endif()

# ---------------------------------------------------------------------------
# Protobuf / gRPC code generation
# ---------------------------------------------------------------------------
set(PROTO_SRC_DIR "${CMAKE_SOURCE_DIR}/proto")
set(PROTO_GEN_DIR "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

set(PROTO_FILE "${PROTO_SRC_DIR}/kvstore.proto")

set(PB_CC  "${PROTO_GEN_DIR}/kvstore.pb.cc")
set(PB_H   "${PROTO_GEN_DIR}/kvstore.pb.h")
set(GRPC_CC "${PROTO_GEN_DIR}/kvstore.grpc.pb.cc")
set(GRPC_H  "${PROTO_GEN_DIR}/kvstore.grpc.pb.h")

add_custom_command(
  OUTPUT  ${PB_CC} ${PB_H} ${GRPC_CC} ${GRPC_H}
  COMMAND protobuf::protoc
    --proto_path=${PROTO_SRC_DIR}
    --cpp_out=${PROTO_GEN_DIR}
    --grpc_out=${PROTO_GEN_DIR}
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
    ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating protobuf and gRPC sources"
)

add_library(kvstore_proto STATIC ${PB_CC} ${GRPC_CC})
target_include_directories(kvstore_proto PUBLIC ${PROTO_GEN_DIR})
target_link_libraries(kvstore_proto
  PUBLIC
    protobuf::libprotobuf
    ${GRPC_LIBRARIES}
)

# ---------------------------------------------------------------------------
# Core library (storage, WAL, consistent hashing, replication)
# ---------------------------------------------------------------------------
add_library(kvstore_core STATIC
  src/server/storage.cpp
  src/server/wal.cpp
  src/server/consistent_hash.cpp
  src/server/replication.cpp
  src/server/node.cpp
)
target_include_directories(kvstore_core
  PUBLIC
    ${CMAKE_SOURCE_DIR}/src/server
    ${PROTO_GEN_DIR}
)
target_link_libraries(kvstore_core
  PUBLIC
    kvstore_proto
    Threads::Threads
)

# ---------------------------------------------------------------------------
# Server executable
# ---------------------------------------------------------------------------
add_executable(kv_server src/server/main.cpp)
target_link_libraries(kv_server PRIVATE kvstore_core)

# ---------------------------------------------------------------------------
# Client executable
# ---------------------------------------------------------------------------
add_executable(kv_client src/client/main.cpp)
target_include_directories(kv_client PRIVATE ${PROTO_GEN_DIR})
target_link_libraries(kv_client
  PRIVATE
    kvstore_proto
    Threads::Threads
)

# ---------------------------------------------------------------------------
# Tests (Google Test)
# ---------------------------------------------------------------------------
if(BUILD_TESTS)
  enable_testing()
  find_package(GTest QUIET)

  if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(
      googletest
      GIT_REPOSITORY https://github.com/google/googletest.git
      GIT_TAG        v1.14.0
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
  endif()

  add_executable(test_storage     tests/test_storage.cpp)
  add_executable(test_consistent_hash tests/test_consistent_hash.cpp)
  add_executable(test_wal         tests/test_wal.cpp)

  foreach(test_target test_storage test_consistent_hash test_wal)
    target_link_libraries(${test_target}
      PRIVATE
        kvstore_core
        GTest::gtest
        GTest::gtest_main
    )
    add_test(NAME ${test_target} COMMAND ${test_target})
  endforeach()
endif()

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------
if(BUILD_BENCHMARKS)
  add_executable(bench_throughput benchmarks/bench_throughput.cpp)
  target_link_libraries(bench_throughput PRIVATE kvstore_core)
endif()
