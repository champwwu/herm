# Market data handlers library
add_library(engine_exchange
  market_data_handler.cpp
  binance_spot_handler.cpp
  binance_futures_handler.cpp
  bybit_handler.cpp
  okx_handler.cpp
  mock_handler.cpp
  market_data_handler_factory.cpp
  rate_limiter.cpp
  base_http_client.cpp
  binance_http_client.cpp
  bybit_http_client.cpp
  okx_http_client.cpp
)

target_include_directories(engine_exchange PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}
  ${GENERATED_PROTOBUF_PATH}
)

target_link_libraries(engine_exchange PUBLIC
  engine_common
  engine_market_data
  Boost::system
  spdlog::spdlog
  nlohmann_json::nlohmann_json
  OpenSSL::SSL
  OpenSSL::Crypto
)
