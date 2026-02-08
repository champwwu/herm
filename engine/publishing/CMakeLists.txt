# Publishing service library
add_library(engine_publishing
  publisher_service.cpp
  order_book_json_converter.cpp
)

target_include_directories(engine_publishing PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}
  ${GENERATED_PROTOBUF_PATH}
)

target_link_libraries(engine_publishing PUBLIC
  engine_common
  market_data_proto
  gRPC::grpc++
  nlohmann_json::nlohmann_json
  spdlog::spdlog
)
