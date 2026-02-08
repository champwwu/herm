cmake_minimum_required(VERSION 3.20)
project(herm LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)

# Enable TRACE and DEBUG log levels in spdlog (disabled by default for performance)
add_compile_definitions(SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)

# Option to control test building (default ON for local builds)
option(BUILD_TESTING "Build the testing tree" ON)

# Output all executables to bin/ directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Enable testing
enable_testing()

# Coverage flags (for gcov/lcov)
option(ENABLE_COVERAGE "Enable code coverage" OFF)
if(ENABLE_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(--coverage)
        add_link_options(--coverage)
        message(STATUS "Code coverage enabled")
    else()
        message(WARNING "Coverage is only supported for GCC and Clang")
    endif()
endif()

# Conan-generated files (run: conan install . --output-folder=build --build=missing)
if(EXISTS "${CMAKE_CURRENT_BINARY_DIR}/conan_toolchain.cmake")
  include(${CMAKE_CURRENT_BINARY_DIR}/conan_toolchain.cmake)
endif()

# Find packages provided by Conan
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(absl REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
# Boost.Beast is header-only, included via boost/beast.hpp
find_package(nlohmann_json REQUIRED)
find_package(spdlog REQUIRED)
find_package(GTest REQUIRED)
find_package(CLI11 REQUIRED)

# Proto and gRPC code generation
set(PROTO_PATH "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(GENERATED_PROTOBUF_PATH "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_PROTOBUF_PATH})

# Protobuf_PROTOC_EXECUTABLE is set by find_package(Protobuf) from Conan
if(NOT Protobuf_PROTOC_EXECUTABLE)
  find_program(Protobuf_PROTOC_EXECUTABLE protoc)
endif()

# Generate demo.proto
set(DEMO_PROTO_FILE "${PROTO_PATH}/demo.proto")
set(DEMO_PB_SRCS "${GENERATED_PROTOBUF_PATH}/demo.pb.cc")
set(DEMO_PB_HDRS "${GENERATED_PROTOBUF_PATH}/demo.pb.h")
add_custom_command(
  OUTPUT ${DEMO_PB_SRCS} ${DEMO_PB_HDRS}
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
  ARGS --cpp_out=${GENERATED_PROTOBUF_PATH} -I${PROTO_PATH} ${DEMO_PROTO_FILE}
  DEPENDS ${DEMO_PROTO_FILE}
  COMMENT "Generating protobuf C++ for demo.proto"
)

set(DEMO_GRPC_SRCS "${GENERATED_PROTOBUF_PATH}/demo.grpc.pb.cc")
set(DEMO_GRPC_HDRS "${GENERATED_PROTOBUF_PATH}/demo.grpc.pb.h")
add_custom_command(
  OUTPUT ${DEMO_GRPC_SRCS} ${DEMO_GRPC_HDRS}
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
  --grpc_out=${GENERATED_PROTOBUF_PATH}
  --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
  -I${PROTO_PATH} ${DEMO_PROTO_FILE}
  DEPENDS ${DEMO_PROTO_FILE} ${DEMO_PB_SRCS} gRPC::grpc_cpp_plugin
  COMMENT "Generating gRPC C++ for demo.proto"
)

add_library(demo_proto
  ${DEMO_PB_SRCS}
  ${DEMO_GRPC_SRCS}
)
target_include_directories(demo_proto PUBLIC
  ${GENERATED_PROTOBUF_PATH}
)
target_link_libraries(demo_proto PUBLIC
  protobuf::libprotobuf
  gRPC::grpc++
  gRPC::grpc++_reflection
)

# Generate market_data.proto
set(MARKET_DATA_PROTO_FILE "${PROTO_PATH}/market_data.proto")
set(MARKET_DATA_PB_SRCS "${GENERATED_PROTOBUF_PATH}/market_data.pb.cc")
set(MARKET_DATA_PB_HDRS "${GENERATED_PROTOBUF_PATH}/market_data.pb.h")
add_custom_command(
  OUTPUT ${MARKET_DATA_PB_SRCS} ${MARKET_DATA_PB_HDRS}
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
  ARGS --cpp_out=${GENERATED_PROTOBUF_PATH} -I${PROTO_PATH} ${MARKET_DATA_PROTO_FILE}
  DEPENDS ${MARKET_DATA_PROTO_FILE}
  COMMENT "Generating protobuf C++ for market_data.proto"
)

set(MARKET_DATA_GRPC_SRCS "${GENERATED_PROTOBUF_PATH}/market_data.grpc.pb.cc")
set(MARKET_DATA_GRPC_HDRS "${GENERATED_PROTOBUF_PATH}/market_data.grpc.pb.h")
add_custom_command(
  OUTPUT ${MARKET_DATA_GRPC_SRCS} ${MARKET_DATA_GRPC_HDRS}
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
  --grpc_out=${GENERATED_PROTOBUF_PATH}
  --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
  -I${PROTO_PATH} ${MARKET_DATA_PROTO_FILE}
  DEPENDS ${MARKET_DATA_PROTO_FILE} ${MARKET_DATA_PB_SRCS} gRPC::grpc_cpp_plugin
  COMMENT "Generating gRPC C++ for market_data.proto"
)

add_library(market_data_proto
  ${MARKET_DATA_PB_SRCS}
  ${MARKET_DATA_GRPC_SRCS}
)
target_include_directories(market_data_proto PUBLIC
  ${GENERATED_PROTOBUF_PATH}
)
target_link_libraries(market_data_proto PUBLIC
  protobuf::libprotobuf
  gRPC::grpc++
  gRPC::grpc++_reflection
)

# Engine subfolders
add_subdirectory(engine/common)
add_subdirectory(engine/market_data)
add_subdirectory(engine/exchange)
add_subdirectory(engine/mock)
add_subdirectory(engine/aggregation)
add_subdirectory(engine/publishing)

# ============================================================================
# Applications
# ============================================================================

# Demo applications (showcasing ApplicationKernel usage with gRPC)
add_executable(demo_server
  apps/demo_server/main.cpp
)
target_include_directories(demo_server PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/engine
  ${GENERATED_PROTOBUF_PATH}
)
target_link_libraries(demo_server PRIVATE
  engine_common
  demo_proto
  Boost::system
  absl::log
  spdlog::spdlog
)

add_executable(demo_client
  apps/demo_client/main.cpp
)
target_include_directories(demo_client PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/engine
  ${GENERATED_PROTOBUF_PATH}
)
target_link_libraries(demo_client PRIVATE
  engine_common
  demo_proto
  Boost::system
  absl::log
  spdlog::spdlog
)

# Mock exchange server executable
add_executable(mock_exchange_server
  apps/mock_exchange_server/main.cpp
)
target_include_directories(mock_exchange_server PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/engine
)
target_link_libraries(mock_exchange_server PRIVATE engine_common)
target_link_libraries(mock_exchange_server PRIVATE
  engine_mock
  Boost::system
  absl::log
  spdlog::spdlog
)

# Aggregator service executable
add_executable(aggregator
  apps/aggregator/main.cpp
)
target_include_directories(aggregator PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/engine
  ${GENERATED_PROTOBUF_PATH}
)
target_link_libraries(aggregator PRIVATE engine_common)
target_link_libraries(aggregator PRIVATE
  engine_aggregation
  market_data_proto
  Boost::system
  absl::log
  spdlog::spdlog
)

# Unified publisher executable
add_executable(publisher
  apps/publisher/main.cpp
)
target_include_directories(publisher PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/engine
  ${GENERATED_PROTOBUF_PATH}
)
target_link_libraries(publisher PRIVATE
  engine_common
  engine_publishing
  market_data_proto
  spdlog::spdlog
)

# Handler client executable
add_executable(handler_client
  apps/handler_client/main.cpp
)
target_include_directories(handler_client PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/engine
  ${GENERATED_PROTOBUF_PATH}
)
target_link_libraries(handler_client PRIVATE engine_common)
target_link_libraries(handler_client PRIVATE
  engine_exchange
  spdlog::spdlog
)

# ============================================================================
# Testing
# ============================================================================
if(BUILD_TESTING)
  enable_testing()

  # Test infrastructure
  # Create tests directory structure placeholder
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tests/unit)
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration)
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures)

# Function to add a test executable
function(add_herm_test test_name)
    add_executable(${test_name} ${ARGN})
    target_include_directories(${test_name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/engine
        ${GENERATED_PROTOBUF_PATH}
    )
    target_link_libraries(${test_name} PRIVATE
        GTest::gtest
        GTest::gtest_main
        GTest::gmock
        Boost::system
        spdlog::spdlog
    )
    add_test(NAME ${test_name} COMMAND ${test_name})
    if(ENABLE_COVERAGE)
        if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
            set_target_properties(${test_name} PROPERTIES
                COMPILE_FLAGS "--coverage"
                LINK_FLAGS "--coverage"
            )
        endif()
    endif()
endfunction()

# ============================================================================
# Unit Tests - Core Data Structures (Priority 1)
# ============================================================================

# Order book tests
add_herm_test(order_book_test
    tests/unit/order_book_test.cpp
)
target_link_libraries(order_book_test PRIVATE engine_market_data)

# Order book aggregator tests
add_herm_test(order_book_aggregator_test
    tests/unit/order_book_aggregator_test.cpp
)
target_link_libraries(order_book_aggregator_test PRIVATE engine_market_data)

# Order book resampler tests
add_herm_test(order_book_resampler_test
    tests/unit/order_book_resampler_test.cpp
)
target_link_libraries(order_book_resampler_test PRIVATE engine_aggregation market_data_proto)

# Config manager tests
add_herm_test(config_manager_test
    tests/unit/config_manager_test.cpp
)
target_link_libraries(config_manager_test PRIVATE engine_common nlohmann_json::nlohmann_json)

# Instrument registry tests
add_herm_test(instrument_registry_test
    tests/unit/instrument_registry_test.cpp
)
target_link_libraries(instrument_registry_test PRIVATE engine_common)

# Rate limiter tests
add_herm_test(rate_limiter_test
    tests/unit/rate_limiter_test.cpp
)
target_link_libraries(rate_limiter_test PRIVATE engine_exchange)

# ============================================================================
# Unit Tests - Service Components (Priority 2)
# ============================================================================

# Order book processor tests (with mocks)
add_herm_test(order_book_processor_test
    tests/unit/order_book_processor_test.cpp
)
target_include_directories(order_book_processor_test PRIVATE tests)
target_link_libraries(order_book_processor_test PRIVATE engine_aggregation market_data_proto)

# ============================================================================
# Unit Tests - Supporting Components
# ============================================================================

# Event thread tests
add_herm_test(event_thread_test
    tests/unit/event_thread_test.cpp
)
target_link_libraries(event_thread_test PRIVATE engine_common)

# RCU pointer tests
add_herm_test(rcu_ptr_test
    tests/unit/rcu_ptr_test.cpp
)
target_link_libraries(rcu_ptr_test PRIVATE engine_common)

# Order book JSON converter tests
add_herm_test(order_book_json_converter_test
    tests/unit/order_book_json_converter_test.cpp
)
target_link_libraries(order_book_json_converter_test PRIVATE engine_publishing market_data_proto nlohmann_json::nlohmann_json)

# Market data handler factory tests
add_herm_test(market_data_handler_factory_test
    tests/unit/market_data_handler_factory_test.cpp
)
target_link_libraries(market_data_handler_factory_test PRIVATE engine_exchange engine_common)

# Mock exchange tests
add_herm_test(mock_exchange_test
    tests/unit/mock_exchange_test.cpp
)
target_link_libraries(mock_exchange_test PRIVATE engine_mock)

# Integration tests (Python)
if(PYTHON_EXECUTABLE)
    add_test(NAME integration_e2e_test
        COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration/testplan_e2e.py
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
else()
    # Try to find Python
    find_program(PYTHON_EXECUTABLE python3 python)
    if(PYTHON_EXECUTABLE)
        add_test(NAME integration_e2e_test
            COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration/testplan_e2e.py
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )
    endif()
endif()

endif() # BUILD_TESTING
