#pragma once

#include "engine/market_data/order_book.hpp"
#include "engine/common/instrument_registry.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <nlohmann/json.hpp>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <map>
#include <set>
#include <vector>
#include <string>
#include <chrono>

namespace herm {

namespace beast = boost::beast;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = boost::asio::ip::tcp;

// Per-instrument order book state (separated from connection)
struct InstrumentOrderBookState {
  std::string instrument_id;
  
  // Order book data
  std::map<double, double, std::greater<double>> local_order_book_bids_;
  std::map<double, double> local_order_book_asks_;
  int64_t last_update_id_ = 0;
  int64_t last_seq_ = 0;  // For Bybit
  bool snapshot_received_ = false;
  std::vector<nlohmann::json> buffered_events_;
  std::mutex order_book_mutex_;
  
  // Reusable buffers (protected by order_book_mutex_) - avoid repeated allocations
  std::vector<PriceLevel> temp_bids_;
  std::vector<PriceLevel> temp_asks_;
  std::vector<nlohmann::json> temp_valid_events_;
  
  // Fragment buffer for accumulating partial WebSocket messages (Bybit)
  std::string fragment_buffer_;
  
  std::chrono::steady_clock::time_point last_update_time_;
  int reconnect_attempt_ = 0;
  std::chrono::steady_clock::time_point next_reconnect_time_;
};

// Shared connection for multiple instruments
struct SharedConnection {
  // WebSocket connection (shared across instruments)
  std::unique_ptr<net::io_context> ioc_;
  std::unique_ptr<ssl::context> ssl_ctx_;
  std::unique_ptr<tcp::resolver> resolver_;
  std::unique_ptr<websocket::stream<ssl::stream<beast::tcp_stream>>> ws_;
  std::thread io_thread_;
  std::atomic<bool> connected_{false};
  std::atomic<bool> running_{false};
  
  // Connection parameters
  std::string host_;
  std::string port_;
  std::string path_;
  
  // Instrument type this connection serves
  engine::common::InstrumentType instrument_type_;
  
  // Set of instrument IDs using this connection
  std::set<std::string> instrument_ids_;
  std::mutex instruments_mutex_;
  
  // Reusable WebSocket buffer
  std::shared_ptr<beast::flat_buffer> ws_buffer_;
  
  std::chrono::steady_clock::time_point last_ping_time_;
  int reconnect_attempt_ = 0;
  std::chrono::steady_clock::time_point next_reconnect_time_;
};

}  // namespace herm
