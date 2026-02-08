# Common engine library
add_library(engine_common
  application_kernel.cpp
  config_manager.cpp
  instrument_registry.cpp
  event_thread.cpp
  rest_server.cpp
  grpc_server.cpp
  util.cpp
  cpu_pinning.cpp
)

target_include_directories(engine_common PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}
)

target_link_libraries(engine_common PUBLIC
  Boost::system
  Boost::boost
  nlohmann_json::nlohmann_json
  spdlog::spdlog
  absl::log
  gRPC::grpc++
  CLI11::CLI11
)
