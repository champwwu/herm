# Mock exchange library
add_library(engine_mock
  mock_exchange.cpp
  mock_exchange_websocket.cpp
)

target_include_directories(engine_mock PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}
)

target_link_libraries(engine_mock PUBLIC
  engine_common
  engine_market_data
  Boost::system
  spdlog::spdlog
  nlohmann_json::nlohmann_json
)
