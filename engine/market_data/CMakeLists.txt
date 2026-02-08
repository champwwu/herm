# Market data library
add_library(engine_market_data
  order_book.cpp
  order_book_aggregator.cpp
)

target_include_directories(engine_market_data PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}
  ${GENERATED_PROTOBUF_PATH}
)

target_link_libraries(engine_market_data PUBLIC
  engine_common
  market_data_proto
)
