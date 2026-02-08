# Aggregation service library
add_library(engine_aggregation
  market_data_manager.cpp
  order_book_processor.cpp
  order_book_resampler.cpp
  aggregator_service.cpp
)

target_include_directories(engine_aggregation PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}
  ${GENERATED_PROTOBUF_PATH}
)

target_link_libraries(engine_aggregation PUBLIC
  engine_common
  engine_market_data
  engine_exchange
  market_data_proto
  Boost::system
  spdlog::spdlog
  nlohmann_json::nlohmann_json
)
