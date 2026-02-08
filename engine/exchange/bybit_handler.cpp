#include "bybit_handler.hpp"
#include "engine/common/util.hpp"
#include "engine/common/cpu_pinning.hpp"
#include <spdlog/spdlog.h>
#include <algorithm>
#include <csignal>
#include <future>
#include <sstream>
#include <random>

namespace herm {

BybitHandler::BybitHandler(const engine::common::ConfigManager& config)
    : MarketDataHandler("bybit"),
      config_(&config),
      http_client_(std::make_unique<BybitHttpClient>()),
      rate_limiter_(std::make_unique<RateLimiter>(40, 40)) {  // Default 40 req/s
  Initialize(config);
}

BybitHandler::~BybitHandler() {
  // Disconnect() should have been called explicitly before destruction
  // Calling it here as safety fallback
  if (spot_connection_ || perp_connection_) {
    SPDLOG_WARN("BybitHandler: Connections not empty in destructor, calling Disconnect()");
    Disconnect();
  }
}

void BybitHandler::Initialize(const engine::common::ConfigManager& config) {
  config_ = &config;
  
  // Parse bybit config section from market_data.bybit
  auto bybit_node = config.GetNodeValue("market_data.bybit");
  if (bybit_node.is_object()) {
    // Spot configuration
    if (bybit_node.contains("spot") && bybit_node["spot"].is_object()) {
      auto spot_node = bybit_node["spot"];
      if (spot_node.contains("websocket_url") && spot_node["websocket_url"].is_string()) {
        websocket_url_spot_ = spot_node["websocket_url"].get<std::string>();
      } else {
        websocket_url_spot_ = "wss://stream.bybit.com/v5/public/spot";
      }
    } else {
      websocket_url_spot_ = "wss://stream.bybit.com/v5/public/spot";
    }
    
    // Perpetual configuration
    if (bybit_node.contains("perpetual") && bybit_node["perpetual"].is_object()) {
      auto perp_node = bybit_node["perpetual"];
      if (perp_node.contains("websocket_url") && perp_node["websocket_url"].is_string()) {
        websocket_url_perp_ = perp_node["websocket_url"].get<std::string>();
      } else {
        websocket_url_perp_ = "wss://stream.bybit.com/v5/public/linear";
      }
    } else {
      websocket_url_perp_ = "wss://stream.bybit.com/v5/public/linear";
    }
    
    // REST URL
    // REST URL
    if (bybit_node.contains("rest_url") && bybit_node["rest_url"].is_string()) {
      rest_url_ = bybit_node["rest_url"].get<std::string>();
      // Set the base URL on the HTTP client
      http_client_->SetBaseUrl(rest_url_);
    } else {
      rest_url_ = "https://api.bybit.com";
    }
    
    // Rate limit
    if (bybit_node.contains("rate_limit") && bybit_node["rate_limit"].is_object()) {
      auto rate_limit_node = bybit_node["rate_limit"];
      if (rate_limit_node.contains("requests_per_second") && rate_limit_node["requests_per_second"].is_number()) {
        rate_limit_rps_ = rate_limit_node["requests_per_second"].get<int>();
        rate_limiter_ = std::make_unique<RateLimiter>(rate_limit_rps_, rate_limit_rps_);
      }
    }
  } else {
    // Use defaults if config not found
    websocket_url_spot_ = "wss://stream.bybit.com/v5/public/spot";
    websocket_url_perp_ = "wss://stream.bybit.com/v5/public/linear";
    rest_url_ = "https://api.bybit.com";
    rate_limit_rps_ = 40;
  }
  
  SPDLOG_INFO("BybitHandler initialized - websocket_url_spot: {}, websocket_url_perp: {}, rest_url: {}, rate_limit: {} req/s",
               websocket_url_spot_, websocket_url_perp_, rest_url_, rate_limit_rps_);
}

bool BybitHandler::Subscribe(const engine::common::Instrument& instrument) {
  if (instrument.instrument_type != engine::common::InstrumentType::SPOT &&
      instrument.instrument_type != engine::common::InstrumentType::PERP) {
    SPDLOG_WARN("BybitHandler: Cannot subscribe non-SPOT/PERP instrument: {}", instrument.instrument_id);
    return false;
  }
  
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    
    // Check if already subscribed
    if (subscribed_instruments_.find(instrument.instrument_id) != subscribed_instruments_.end()) {
      SPDLOG_WARN("BybitHandler: Already subscribed to instrument: {}", instrument.instrument_id);
      return false;
    }
    
    // Add to subscribed instruments
    subscribed_instruments_[instrument.instrument_id] = &instrument;
  }
  
  // Create symbol mapping
  std::string exchange_symbol = GetBybitSymbol(instrument);
  {
    std::lock_guard<std::mutex> lock(symbol_mapping_mutex_);
    symbol_to_instrument_id_[exchange_symbol] = instrument.instrument_id;
  }
  
  // Initialize order book state
  {
    std::lock_guard<std::mutex> lock(order_books_mutex_);
    auto& order_book_state = order_books_[instrument.instrument_id];
    order_book_state.instrument_id = instrument.instrument_id;
    order_book_state.buffered_events_.reserve(32);
    order_book_state.temp_valid_events_.reserve(32);
  }
  
  SPDLOG_INFO("BybitHandler: Subscribed to instrument: {}", instrument.instrument_id);
  
  // Subscribe on shared connection
  return SubscribeInstrument(instrument);
}

bool BybitHandler::Unsubscribe(const std::string& instrument_id) {
  const engine::common::Instrument* instrument = nullptr;
  
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    auto it = subscribed_instruments_.find(instrument_id);
    if (it == subscribed_instruments_.end()) {
      SPDLOG_WARN("BybitHandler: Not subscribed to instrument: {}", instrument_id);
      return false;
    }
    instrument = it->second;
    subscribed_instruments_.erase(it);
  }
  
  // Unsubscribe from shared connection
  if (instrument) {
    UnsubscribeInstrument(instrument_id);
  }
  
  // Remove symbol mapping
  if (instrument) {
    std::string exchange_symbol = GetBybitSymbol(*instrument);
    std::lock_guard<std::mutex> lock(symbol_mapping_mutex_);
    symbol_to_instrument_id_.erase(exchange_symbol);
  }
  
  // Remove order book state
  {
    std::lock_guard<std::mutex> lock(order_books_mutex_);
    order_books_.erase(instrument_id);
  }
  
  SPDLOG_DEBUG("BybitHandler: Unsubscribed from instrument: {}", instrument_id);
  return true;
}

bool BybitHandler::Connect() {
  bool spot_connected = EnsureConnected(engine::common::InstrumentType::SPOT);
  bool perp_connected = EnsureConnected(engine::common::InstrumentType::PERP);
  return spot_connected && perp_connected;
}

void BybitHandler::Disconnect() {
  DisconnectSharedConnection(engine::common::InstrumentType::SPOT);
  DisconnectSharedConnection(engine::common::InstrumentType::PERP);
  
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    subscribed_instruments_.clear();
  }
  
  {
    std::lock_guard<std::mutex> lock(order_books_mutex_);
    order_books_.clear();
  }
  
  {
    std::lock_guard<std::mutex> lock(symbol_mapping_mutex_);
    symbol_to_instrument_id_.clear();
  }
}

bool BybitHandler::IsConnected() const {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  bool spot_connected = spot_connection_ && spot_connection_->connected_.load();
  bool perp_connected = perp_connection_ && perp_connection_->connected_.load();
  return spot_connected || perp_connected;
}

SharedConnection* BybitHandler::GetOrCreateSharedConnection(engine::common::InstrumentType type) {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  
  std::unique_ptr<SharedConnection>& conn_ref = (type == engine::common::InstrumentType::SPOT) ? spot_connection_ : perp_connection_;
  
  if (!conn_ref) {
    conn_ref = std::make_unique<SharedConnection>();
    conn_ref->instrument_type_ = type;
    conn_ref->ws_buffer_ = std::make_shared<beast::flat_buffer>();
    conn_ref->ws_buffer_->reserve(4096);
  }
  
  return conn_ref.get();
}

bool BybitHandler::EnsureConnected(engine::common::InstrumentType type) {
  auto* conn = GetOrCreateSharedConnection(type);
  if (!conn) {
    return true;  // No connection needed for this type
  }
  
  if (conn->connected_.load()) {
    return true;
  }
  
  // Get all subscribed instruments of this type
  std::vector<const engine::common::Instrument*> instruments;
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    for (const auto& [id, inst] : subscribed_instruments_) {
      if (inst->instrument_type == type) {
        instruments.push_back(inst);
      }
    }
  }
  
  if (instruments.empty()) {
    return true;  // Nothing to connect
  }
  
  return ConnectSharedConnectionWithRetry(type, instruments);
}

void BybitHandler::DisconnectSharedConnection(engine::common::InstrumentType type) {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  
  std::unique_ptr<SharedConnection>& conn_ref = (type == engine::common::InstrumentType::SPOT) ? spot_connection_ : perp_connection_;
  
  if (conn_ref && conn_ref->running_.exchange(false)) {
    if (conn_ref->ws_ && conn_ref->ws_->is_open()) {
      beast::error_code ec;
      conn_ref->ws_->close(websocket::close_code::normal, ec);
    }
    if (conn_ref->ioc_) {
      conn_ref->ioc_->stop();
    }
    if (conn_ref->io_thread_.joinable()) {
      conn_ref->io_thread_.join();
    }
    conn_ref->connected_ = false;
    SPDLOG_DEBUG("BybitHandler: {} shared connection disconnected", 
                 type == engine::common::InstrumentType::SPOT ? "SPOT" : "PERP");
  }
  
  conn_ref.reset();
}

std::string BybitHandler::GetBybitSymbol(const engine::common::Instrument& instrument) const {
  // Bybit uses uppercase symbols (e.g., "BTCUSDT")
  return instrument.GetSymbol();
}

std::string BybitHandler::GetWebSocketUrl(const engine::common::Instrument& instrument) const {
  if (instrument.instrument_type == engine::common::InstrumentType::SPOT) {
    return websocket_url_spot_;
  } else {
    return websocket_url_perp_;
  }
}

std::string BybitHandler::BuildSubscriptionMessage(const std::vector<const engine::common::Instrument*>& instruments) const {
  nlohmann::json sub_msg;
  sub_msg["op"] = "subscribe";
  sub_msg["args"] = nlohmann::json::array();
  
  for (const auto* inst : instruments) {
    std::string symbol = GetBybitSymbol(*inst);
    // Use depth 200 for orderbook (20ms update frequency)
    sub_msg["args"].push_back("orderbook.200." + symbol);
  }
  
  return sub_msg.dump();
}

std::string BybitHandler::BuildUnsubscriptionMessage(const std::string& exchange_symbol) const {
  nlohmann::json unsub_msg;
  unsub_msg["op"] = "unsubscribe";
  unsub_msg["args"] = nlohmann::json::array();
  unsub_msg["args"].push_back("orderbook.200." + exchange_symbol);
  return unsub_msg.dump();
}

InstrumentOrderBookState* BybitHandler::FindOrderBookBySymbol(const std::string& exchange_symbol) {
  // Find instrument ID from exchange symbol
  std::string instrument_id;
  {
    std::lock_guard<std::mutex> lock(symbol_mapping_mutex_);
    auto it = symbol_to_instrument_id_.find(exchange_symbol);
    if (it == symbol_to_instrument_id_.end()) {
      return nullptr;
    }
    instrument_id = it->second;
  }
  
  // Find order book state
  std::lock_guard<std::mutex> lock(order_books_mutex_);
  auto it = order_books_.find(instrument_id);
  if (it == order_books_.end()) {
    return nullptr;
  }
  
  return &it->second;
}

std::string BybitHandler::GetInstrumentIdFromSymbol(const std::string& exchange_symbol) const {
  std::lock_guard<std::mutex> lock(symbol_mapping_mutex_);
  auto it = symbol_to_instrument_id_.find(exchange_symbol);
  if (it != symbol_to_instrument_id_.end()) {
    return it->second;
  }
  return "";
}

bool BybitHandler::ConnectSharedConnection(engine::common::InstrumentType type, const std::vector<const engine::common::Instrument*>& instruments) {
  auto* conn = GetOrCreateSharedConnection(type);
  if (!conn) {
    return false;
  }
  
  if (conn->connected_.load()) {
    return true;
  }

  try {
    conn->ioc_ = std::make_unique<net::io_context>();
    conn->ssl_ctx_ = std::make_unique<ssl::context>(ssl::context::tlsv12_client);
    conn->ssl_ctx_->set_default_verify_paths();
    conn->ssl_ctx_->set_verify_mode(ssl::verify_none);

    conn->resolver_ = std::make_unique<tcp::resolver>(*conn->ioc_);
    conn->ws_ = std::make_unique<websocket::stream<ssl::stream<beast::tcp_stream>>>(
        *conn->ioc_, *conn->ssl_ctx_);

    // Get WebSocket URL based on instrument type
    std::string url = (type == engine::common::InstrumentType::SPOT) ? websocket_url_spot_ : websocket_url_perp_;
    
    // Parse URL using utility
    auto parsed_url = engine::common::ParseWebSocketUrl(url, "stream.bybit.com", "443");
    conn->host_ = parsed_url.host;
    conn->port_ = parsed_url.port;
    conn->path_ = parsed_url.path;

    std::string type_str = (type == engine::common::InstrumentType::SPOT) ? "SPOT" : "PERP";
    SPDLOG_DEBUG("BybitHandler: Connecting {} shared connection to {}:{}{}",
                 type_str, conn->host_, conn->port_, conn->path_);

    conn->resolver_->async_resolve(conn->host_, conn->port_,
        [this, conn, type](beast::error_code ec, tcp::resolver::results_type results) {
          if (ec) {
            SPDLOG_ERROR("BybitHandler {} shared connection resolve error: {}", 
                        type == engine::common::InstrumentType::SPOT ? "SPOT" : "PERP", ec.message());
            conn->running_ = false;
            if (conn->ioc_) {
              conn->ioc_->stop();
            }
            return;
          }
          beast::get_lowest_layer(*conn->ws_).async_connect(results,
              [this, conn, type](beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
                if (ec) {
                  SPDLOG_ERROR("BybitHandler {} shared connection connect error: {}",
                              type == engine::common::InstrumentType::SPOT ? "SPOT" : "PERP", ec.message());
                  conn->running_ = false;
                  if (conn->ioc_) {
                    conn->ioc_->stop();
                  }
                  return;
                }
                // Set SNI (Server Name Indication) for SSL
                SSL_set_tlsext_host_name(conn->ws_->next_layer().native_handle(), conn->host_.c_str());
                // SSL handshake
                conn->ws_->next_layer().async_handshake(ssl::stream_base::client,
                    [this, conn, type](beast::error_code ec) {
                      if (ec) {
                        SPDLOG_ERROR("BybitHandler {} shared connection SSL handshake error: {}",
                                    type == engine::common::InstrumentType::SPOT ? "SPOT" : "PERP", ec.message());
                        conn->running_ = false;
                        if (conn->ioc_) {
                          conn->ioc_->stop();
                        }
                        return;
                      }
                      // WebSocket handshake
                      conn->ws_->async_handshake(conn->host_, conn->path_,
                          [this, conn, type](beast::error_code ec) {
                            OnHandshake(conn, type, ec);
                          });
                    });
              });
        });

    conn->running_ = true;
    conn->io_thread_ = std::thread(&BybitHandler::RunIO, this, conn);

    // Wait for connection
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    bool connected = conn->connected_.load();
    if (!connected && conn->running_.load()) {
      // Connection failed, stop IO thread
      conn->running_ = false;
      if (conn->ioc_) {
        conn->ioc_->stop();
      }
    }
    return connected;
  } catch (const std::exception& e) {
    SPDLOG_ERROR("BybitHandler {} shared connection error: {}", 
                type == engine::common::InstrumentType::SPOT ? "SPOT" : "PERP", e.what());
    return false;
  }
}

bool BybitHandler::ConnectSharedConnectionWithRetry(engine::common::InstrumentType type, const std::vector<const engine::common::Instrument*>& instruments, int max_retries) {
  std::string type_str = (type == engine::common::InstrumentType::SPOT) ? "SPOT" : "PERP";
  
  for (int attempt = 0; attempt < max_retries; ++attempt) {
    if (attempt > 0) {
      // Exponential backoff: 1s, 2s, 4s
      int delay_ms = (1 << (attempt - 1)) * 1000;
      SPDLOG_INFO("BybitHandler: Retry attempt {}/{} for {} shared connection after {}ms",
                  attempt + 1, max_retries, type_str, delay_ms);
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }
    
    try {
      if (ConnectSharedConnection(type, instruments)) {
        SPDLOG_INFO("BybitHandler: Successfully connected {} shared connection on attempt {}/{}",
                    type_str, attempt + 1, max_retries);
        return true;
      }
    } catch (const std::exception& e) {
      SPDLOG_WARN("BybitHandler: {} shared connection attempt {}/{} failed: {}",
                  type_str, attempt + 1, max_retries, e.what());
    }
  }
  
  SPDLOG_ERROR("BybitHandler: Failed to connect {} shared connection after {} attempts",
              type_str, max_retries);
  return false;
}

bool BybitHandler::SubscribeInstrument(const engine::common::Instrument& instrument) {
  auto* conn = GetOrCreateSharedConnection(instrument.instrument_type);
  if (!conn) {
    return false;
  }
  
  // Ensure connection is established
  if (!EnsureConnected(instrument.instrument_type)) {
    SPDLOG_WARN("BybitHandler: Failed to establish shared connection for subscribing {}", instrument.instrument_id);
    return false;
  }
  
  // Send subscription message for this instrument
  std::string exchange_symbol = GetBybitSymbol(instrument);
  nlohmann::json sub_msg;
  sub_msg["op"] = "subscribe";
  sub_msg["args"] = nlohmann::json::array();
  sub_msg["args"].push_back("orderbook.200." + exchange_symbol);
  
  std::string msg = sub_msg.dump();
  
  try {
    conn->ws_->write(net::buffer(msg));
    SPDLOG_DEBUG("BybitHandler: Sent subscription for {} ({})", instrument.instrument_id, exchange_symbol);
    return true;
  } catch (const std::exception& e) {
    SPDLOG_ERROR("BybitHandler: Failed to send subscription for {}: {}", instrument.instrument_id, e.what());
    return false;
  }
}

void BybitHandler::UnsubscribeInstrument(const std::string& instrument_id) {
  const engine::common::Instrument* instrument = FindInstrument(instrument_id);
  if (!instrument) {
    return;
  }
  
  auto* conn = GetOrCreateSharedConnection(instrument->instrument_type);
  if (!conn || !conn->connected_.load()) {
    return;
  }
  
  std::string exchange_symbol = GetBybitSymbol(*instrument);
  std::string msg = BuildUnsubscriptionMessage(exchange_symbol);
  
  try {
    conn->ws_->write(net::buffer(msg));
    SPDLOG_DEBUG("BybitHandler: Sent unsubscription for {} ({})", instrument_id, exchange_symbol);
  } catch (const std::exception& e) {
    SPDLOG_ERROR("BybitHandler: Failed to send unsubscription for {}: {}", instrument_id, e.what());
  }
}

void BybitHandler::RunIO(SharedConnection* conn) {
  SPDLOG_DEBUG("BybitHandler: IO thread started for shared connection");
  
  // Pin to CPU core if manager is set
  if (cpu_pinning_manager_) {
    try {
      cpu_pinning_manager_->PinCurrentThread("bybit_io");
    } catch (const std::exception& e) {
      SPDLOG_ERROR("Failed to pin bybit_io thread: {}", e.what());
      conn->running_ = false;
      if (conn->ioc_) {
        conn->ioc_->stop();
      }
      return;
    }
  }
  
  if (conn->ioc_) {
    try {
      // Run in short slices so shutdown can be observed promptly
      while (conn->running_.load() && conn->ioc_) {
        conn->ioc_->run_for(std::chrono::milliseconds(200));
        if (conn->running_.load()) {
          conn->ioc_->restart();
        }
      }
    } catch (const std::exception& e) {
      SPDLOG_ERROR("BybitHandler: IO thread exception for shared connection: {}", e.what());
    }
  }
  SPDLOG_DEBUG("BybitHandler: IO thread exiting for shared connection");
}

void BybitHandler::OnHandshake(SharedConnection* conn, engine::common::InstrumentType type, beast::error_code ec) {
  if (ec) {
    SPDLOG_ERROR("BybitHandler {} shared connection handshake error: {}",
                type == engine::common::InstrumentType::SPOT ? "SPOT" : "PERP", ec.message());
    return;
  }

  conn->connected_ = true;
  SPDLOG_INFO("BybitHandler: {} shared connection established",
              type == engine::common::InstrumentType::SPOT ? "SPOT" : "PERP");

  // Get all subscribed instruments of this type
  std::vector<const engine::common::Instrument*> instruments;
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    for (const auto& [id, inst] : subscribed_instruments_) {
      if (inst->instrument_type == type) {
        instruments.push_back(inst);
      }
    }
  }

  if (instruments.empty()) {
    SPDLOG_WARN("BybitHandler: No instruments to subscribe on {} shared connection",
                type == engine::common::InstrumentType::SPOT ? "SPOT" : "PERP");
    DoRead(conn);
    return;
  }

  // Send subscription message for all instruments
  std::string sub_msg = BuildSubscriptionMessage(instruments);
  beast::error_code write_ec;
  conn->ws_->write(net::buffer(sub_msg), write_ec);
  if (write_ec) {
    SPDLOG_ERROR("BybitHandler: Failed to send subscriptions: {}", write_ec.message());
    return;
  }
  
  SPDLOG_DEBUG("BybitHandler: Sent subscription message for {} instruments", instruments.size());

  // Get snapshots for all instruments
  for (const auto* inst : instruments) {
    GetSnapshot(*inst);
  }

  // Start reading
  DoRead(conn);
}

void BybitHandler::DoRead(SharedConnection* conn) {
  if (!conn->running_.load() || !conn->ws_ || !conn->ws_->is_open()) {
    SPDLOG_DEBUG("BybitHandler: DoRead skipped - running={}, ws={}, is_open={}", 
                 conn->running_.load(), (conn->ws_ != nullptr), 
                 (conn->ws_ && conn->ws_->is_open()));
    return;
  }

  // Beast WebSocket async_read automatically reassembles fragmented messages
  // The buffer will contain the complete message when the callback is invoked
  // Reuse pre-allocated buffer instead of creating new one
  conn->ws_buffer_->clear();
  conn->ws_->async_read(*conn->ws_buffer_,
      [this, conn](beast::error_code ec, std::size_t bytes_transferred) {
        OnRead(conn, ec, bytes_transferred, std::move(*conn->ws_buffer_));
      });
}

void BybitHandler::OnRead(SharedConnection* conn, beast::error_code ec, std::size_t bytes_transferred, beast::flat_buffer buffer) {
  // Don't process if we're shutting down
  if (!conn->running_.load()) {
    return;
  }
  
  if (ec == websocket::error::closed || ec == net::error::operation_aborted) {
    SPDLOG_DEBUG("BybitHandler: Shared connection WebSocket closed: {}", ec.message());
    conn->connected_ = false;
    return;  // Don't reconnect on normal close
  }

  if (ec) {
    SPDLOG_ERROR("BybitHandler: Shared connection read error: {}", ec.message());
    // Trigger reconnection only if still running
    if (conn->running_.load()) {
      ReconnectAll(conn->instrument_type_);
    }
    return;
  }

  // Check if it's a text message (not binary/ping/pong)
  if (!conn->ws_->got_text()) {
    SPDLOG_DEBUG("BybitHandler: Received non-text frame (binary/ping/pong) on shared connection, skipping");
    DoRead(conn);
    return;
  }

  std::string message(static_cast<const char*>(buffer.data().data()), buffer.size());
  SPDLOG_TRACE("BybitHandler: Received {} bytes on shared connection: [{}...]", 
               bytes_transferred, message.substr(0, std::min<size_t>(100, message.size())));

  try {
    auto json = nlohmann::json::parse(message);

    // Handle subscription success response
    if (json.contains("success") && json["success"].get<bool>() == true) {
      SPDLOG_INFO("BybitHandler: Subscription confirmed on shared connection");
      DoRead(conn);
      return;
    }

    // Handle ping/pong
    if (json.contains("op") && json["op"] == "pong") {
      HandlePing(conn);
      DoRead(conn);
      return;
    }

    // Handle orderbook snapshot and delta
    if (json.contains("topic") && json.contains("type") && json.contains("data")) {
      std::string topic = json["topic"].get<std::string>();
      std::string type = json["type"].get<std::string>();
      
      if (topic.find("orderbook") != std::string::npos) {
        // Extract symbol from topic (e.g., "orderbook.200.BTCUSDT" -> "BTCUSDT")
        size_t last_dot = topic.find_last_of('.');
        if (last_dot == std::string::npos) {
          SPDLOG_WARN("BybitHandler: Invalid topic format: {}", topic);
          DoRead(conn);
          return;
        }
        std::string exchange_symbol = topic.substr(last_dot + 1);
        
        // Find order book state for this instrument
        auto* order_book_state = FindOrderBookBySymbol(exchange_symbol);
        if (!order_book_state) {
          SPDLOG_WARN("BybitHandler: Received message for unknown instrument: {}", exchange_symbol);
          DoRead(conn);
          return;
        }
        
        if (type == "snapshot") {
          // According to Bybit docs: WebSocket snapshot establishes the update ID sequence
          // Always apply WebSocket snapshot to set the correct baseline for orderbook.200 level
          std::lock_guard<std::mutex> lock(order_books_mutex_);
          if (!order_book_state->snapshot_received_) {
            // Buffer event until REST snapshot is received (for order book data)
            order_book_state->buffered_events_.push_back(json);
            SPDLOG_DEBUG("BybitHandler: Buffering WebSocket snapshot (REST snapshot not received yet) for {}",
                        order_book_state->instrument_id);
          } else {
            // Apply WebSocket snapshot directly - this sets the correct update ID sequence
            SPDLOG_TRACE("BybitHandler: Processing WebSocket snapshot event for {}",
                        order_book_state->instrument_id);
            ApplyWebSocketSnapshot(order_book_state, json);
          }
        } else if (type == "delta") {
          if (!order_book_state->snapshot_received_) {
            // Buffer event until REST snapshot is received
            std::lock_guard<std::mutex> lock(order_books_mutex_);
            order_book_state->buffered_events_.push_back(json);
            SPDLOG_DEBUG("BybitHandler: Buffering delta (REST snapshot not received yet) for {}",
                        order_book_state->instrument_id);
          } else {
            // Apply update directly - validation will use WebSocket sequence
            SPDLOG_TRACE("BybitHandler: Processing delta event for {}", order_book_state->instrument_id);
            ApplyUpdate(order_book_state, json);
          }
        }
      }
    }
    
    // Continue reading after successfully processing message
    DoRead(conn);
  } catch (const nlohmann::json::exception& e) {
    SPDLOG_WARN("BybitHandler: JSON parse error on shared connection: {}", e.what());
    // Continue reading even after parse error
    DoRead(conn);
  } catch (const std::exception& e) {
    SPDLOG_WARN("BybitHandler: Parse error on shared connection: {}", e.what());
    // Continue reading even after parse error
    DoRead(conn);
  }
}

void BybitHandler::HandlePing(SharedConnection* conn) {
  if (!conn->ws_ || !conn->ws_->is_open()) return;

  // Bybit sends ping, we respond with pong
  nlohmann::json pong_msg;
  pong_msg["op"] = "pong";

  std::string pong_str = pong_msg.dump();
  beast::error_code ec;
  conn->ws_->write(net::buffer(pong_str), ec);
  if (ec) {
    SPDLOG_WARN("BybitHandler: Error sending pong on shared connection: {}", ec.message());
  } else {
    conn->last_ping_time_ = std::chrono::steady_clock::now();
    SPDLOG_TRACE("BybitHandler: Sent pong on shared connection");
  }
}

void BybitHandler::GetSnapshot(const engine::common::Instrument& instrument) {
  SPDLOG_INFO("BybitHandler: GetSnapshot called for {}", instrument.instrument_id);
  std::thread([this, inst_id = instrument.instrument_id, inst_type = instrument.instrument_type]() {
    SPDLOG_INFO("BybitHandler: GetSnapshot thread started for {}", inst_id);
    // Add small random delay (0-500ms) to stagger snapshot requests
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 500);
    std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));
    
    // Rate limit
    rate_limiter_->Acquire();

    // Get instrument again to ensure it's still subscribed
    const engine::common::Instrument* instrument_ptr = FindInstrument(inst_id);
    if (!instrument_ptr) {
      SPDLOG_WARN("BybitHandler: Instrument {} no longer subscribed, skipping snapshot", inst_id);
      return;
    }

    std::string symbol = GetBybitSymbol(*instrument_ptr);
    bool is_spot = (inst_type == engine::common::InstrumentType::SPOT);
    SPDLOG_INFO("BybitHandler: Fetching snapshot for symbol {} ({})", symbol, inst_id);
    auto snapshot = http_client_->GetDepthSnapshot(symbol, is_spot);

    if (snapshot.lastUpdateId == 0 || snapshot.bids.empty() || snapshot.asks.empty()) {
      SPDLOG_ERROR("BybitHandler: Failed to get snapshot for {}", inst_id);
      // Retry with exponential backoff
      std::this_thread::sleep_for(std::chrono::seconds(1));
      GetSnapshot(*instrument_ptr);
      return;
    }

    SPDLOG_INFO("BybitHandler: Snapshot retrieved, calling ApplySnapshot for {}", inst_id);
    ApplySnapshot(inst_id, snapshot);
    SPDLOG_INFO("BybitHandler: ApplySnapshot completed for {}", inst_id);
  }).detach();
}

void BybitHandler::ApplySnapshot(const std::string& instrument_id, const BybitHttpClient::DepthSnapshot& snapshot) {
  SPDLOG_INFO("BybitHandler: ApplySnapshot started for {}", instrument_id);
  
  InstrumentOrderBookState* order_book_state = nullptr;
  // Extract buffered events and prepare data under lock
  // Reuse temp_valid_events_ buffer
  {
    std::lock_guard<std::mutex> lock(order_books_mutex_);
    auto it = order_books_.find(instrument_id);
    if (it == order_books_.end()) {
      SPDLOG_ERROR("BybitHandler: Order book state not found for {}", instrument_id);
      return;
    }
    order_book_state = &it->second;

    // Clear existing order book
    order_book_state->local_order_book_bids_.clear();
    order_book_state->local_order_book_asks_.clear();

    // Apply snapshot
    SPDLOG_DEBUG("BybitHandler: Applying {} bids and {} asks from snapshot", snapshot.bids.size(), snapshot.asks.size());
    for (const auto& bid : snapshot.bids) {
      order_book_state->local_order_book_bids_[bid.price] = bid.quantity;
    }
    for (const auto& ask : snapshot.asks) {
      order_book_state->local_order_book_asks_[ask.price] = ask.quantity;
    }

    // Reset update ID tracking - WebSocket messages will establish their own sequence
    // REST API and WebSocket use different update ID sequences, so we can't compare them
    // We store the REST snapshot for the order book data, but reset tracking for WebSocket sequence
    order_book_state->last_update_id_ = 0;
    order_book_state->last_seq_ = 0;
    order_book_state->snapshot_received_ = true;

    SPDLOG_DEBUG("BybitHandler: REST Snapshot applied for {}, REST lastUpdateId: {} (will use WebSocket sequence), bids: {}, asks: {}",
                instrument_id, snapshot.lastUpdateId, 
                order_book_state->local_order_book_bids_.size(), order_book_state->local_order_book_asks_.size());

    // Copy buffered events to process outside lock
    // Reuse temp_valid_events_ buffer
    order_book_state->temp_valid_events_.clear();
    order_book_state->temp_valid_events_ = std::move(order_book_state->buffered_events_);
    order_book_state->buffered_events_.clear();
    
    // Copy order book for initial callback
    // Reuse temp_bids_/temp_asks_ buffers
    order_book_state->temp_bids_.clear();
    order_book_state->temp_asks_.clear();
    order_book_state->temp_bids_.reserve(order_book_state->local_order_book_bids_.size());
    order_book_state->temp_asks_.reserve(order_book_state->local_order_book_asks_.size());
    
    for (const auto& [price, qty] : order_book_state->local_order_book_bids_) {
      order_book_state->temp_bids_.emplace_back(price, qty);
    }
    for (const auto& [price, qty] : order_book_state->local_order_book_asks_) {
      order_book_state->temp_asks_.emplace_back(price, qty);
    }
  }  // Release lock
  
  // Apply buffered events outside the lock (they will acquire their own locks)
  SPDLOG_TRACE("BybitHandler: Applying {} buffered events", order_book_state->temp_valid_events_.size());
  for (const auto& event : order_book_state->temp_valid_events_) {
    if (!event.contains("data") || !event["data"].is_object()) {
      continue;
    }
    
    if (event.contains("type") && event["type"] == "snapshot") {
      // Apply WebSocket snapshot - this will set the WebSocket update ID sequence
      ApplyWebSocketSnapshot(order_book_state, event);
    } else if (event.contains("type") && event["type"] == "delta") {
      // Apply delta - validation will accept it since last_update_id_ is 0
      ApplyUpdate(order_book_state, event);
    }
  }
  
  // Send initial order book to callback (outside any locks)
  SPDLOG_INFO("BybitHandler: Looking up instrument for {}", instrument_id);
  const engine::common::Instrument* instrument = FindInstrument(instrument_id);
  if (instrument) {
    SPDLOG_INFO("BybitHandler: Instrument found, calling NotifyUpdate with {} bids, {} asks", order_book_state->temp_bids_.size(), order_book_state->temp_asks_.size());
    NotifyUpdate(*instrument, order_book_state->temp_bids_, order_book_state->temp_asks_);
    SPDLOG_INFO("BybitHandler: NotifyUpdate completed, ApplySnapshot finished");
  } else {
    SPDLOG_ERROR("BybitHandler: Instrument {} not found for callback", instrument_id);
  }
}

void BybitHandler::ApplyWebSocketSnapshot(InstrumentOrderBookState* order_book_state, const nlohmann::json& event) {
  if (!order_book_state) {
    return;
  }
  
  std::string instrument_id = order_book_state->instrument_id;
  std::lock_guard<std::mutex> lock(order_books_mutex_);
  
  if (!event.contains("data") || !event["data"].is_object()) {
    return;
  }
  
  auto data = event["data"];
  
  // Clear and re-initialize order book from WebSocket snapshot
  order_book_state->local_order_book_bids_.clear();
  order_book_state->local_order_book_asks_.clear();
  
  if (data.contains("b") && data["b"].is_array()) {
    for (const auto& level : data["b"]) {
      if (level.is_array() && level.size() >= 2) {
        double price = std::stod(level[0].get<std::string>());
        double qty = std::stod(level[1].get<std::string>());
        if (qty > 0.0) {
          order_book_state->local_order_book_bids_[price] = qty;
        }
      }
    }
  }
  
  if (data.contains("a") && data["a"].is_array()) {
    for (const auto& level : data["a"]) {
      if (level.is_array() && level.size() >= 2) {
        double price = std::stod(level[0].get<std::string>());
        double qty = std::stod(level[1].get<std::string>());
        if (qty > 0.0) {
          order_book_state->local_order_book_asks_[price] = qty;
        }
      }
    }
  }
  
  if (data.contains("u") && data["u"].is_number()) {
    order_book_state->last_update_id_ = data["u"].get<int64_t>();
  }
  if (data.contains("seq") && data["seq"].is_number()) {
    order_book_state->last_seq_ = data["seq"].get<int64_t>();
  }
  
  order_book_state->snapshot_received_ = true;
  
  // Send update to callback
  // Reuse temp_bids_/temp_asks_ buffers
  order_book_state->temp_bids_.clear();
  order_book_state->temp_asks_.clear();
  order_book_state->temp_bids_.reserve(order_book_state->local_order_book_bids_.size());
  order_book_state->temp_asks_.reserve(order_book_state->local_order_book_asks_.size());
  
  for (const auto& [price, qty] : order_book_state->local_order_book_bids_) {
    order_book_state->temp_bids_.emplace_back(price, qty);
  }
  for (const auto& [price, qty] : order_book_state->local_order_book_asks_) {
    order_book_state->temp_asks_.emplace_back(price, qty);
  }
  
  const engine::common::Instrument* instrument = FindInstrument(instrument_id);
  if (instrument) {
    SPDLOG_DEBUG("BybitHandler: Sending WebSocket snapshot update - {} bids, {} asks", order_book_state->temp_bids_.size(), order_book_state->temp_asks_.size());
    NotifyUpdate(*instrument, order_book_state->temp_bids_, order_book_state->temp_asks_);
  }
}

void BybitHandler::ApplyUpdate(InstrumentOrderBookState* order_book_state, const nlohmann::json& event) {
  if (!order_book_state) {
    return;
  }
  
  std::string instrument_id = order_book_state->instrument_id;
  
  {
    std::lock_guard<std::mutex> lock(order_books_mutex_);

    if (!order_book_state->snapshot_received_) {
      return;
    }

    if (!event.contains("data") || !event["data"].is_object()) {
      return;
    }

    auto data = event["data"];
    
    // Extract update IDs
    int64_t update_id = 0;
    int64_t seq = 0;
    if (data.contains("u") && data["u"].is_number()) {
      update_id = data["u"].get<int64_t>();
    }
    if (data.contains("seq") && data["seq"].is_number()) {
      seq = data["seq"].get<int64_t>();
    }

    // According to Bybit documentation:
    // - REST API `u` corresponds to 1000-level WebSocket stream `u`
    // - We subscribe to orderbook.200, so REST and WebSocket use different sequences
    // - We should use WebSocket snapshot's `u` as baseline, not REST snapshot's
    // Detect if we're still using REST snapshot ID (typically > 10M) vs WebSocket ID (typically < 10M)
    // and reset to establish WebSocket sequence
    if (update_id > 0) {
      if (order_book_state->last_update_id_ == 0) {
        // First WebSocket message after REST snapshot reset - establish sequence
        SPDLOG_DEBUG("BybitHandler: First WebSocket delta received (u={}), establishing WebSocket sequence", update_id);
        order_book_state->last_update_id_ = update_id - 1;  // Set to one less so this delta passes validation
      } else if (order_book_state->last_update_id_ > 10000000 && update_id < 10000000) {
        // Detected REST snapshot ID vs WebSocket ID mismatch - reset to WebSocket sequence
        SPDLOG_DEBUG("BybitHandler: Detected REST snapshot ID ({}) vs WebSocket ID ({}), resetting to WebSocket sequence", 
                     order_book_state->last_update_id_, update_id);
        order_book_state->last_update_id_ = update_id - 1;
        order_book_state->last_seq_ = 0;
      }
    }

    // Validate update IDs
    if (!ValidateUpdateId(order_book_state, update_id, seq)) {
      SPDLOG_WARN("BybitHandler: Update ID validation failed for {} (u={}, seq={}, last_u={}, last_seq={}), reinitializing order book", 
                  instrument_id, update_id, seq, order_book_state->last_update_id_, order_book_state->last_seq_);
      order_book_state->snapshot_received_ = false;
      order_book_state->buffered_events_.clear();
      const engine::common::Instrument* instrument = FindInstrument(instrument_id);
      if (instrument) {
        GetSnapshot(*instrument);
      }
      return;
    }

    // Apply bids (delta: size=0 means delete, otherwise insert/update)
    if (data.contains("b") && data["b"].is_array()) {
      for (const auto& level : data["b"]) {
        if (level.is_array() && level.size() >= 2) {
          double price = std::stod(level[0].get<std::string>());
          double qty = std::stod(level[1].get<std::string>());
          if (qty == 0.0) {
            order_book_state->local_order_book_bids_.erase(price);
          } else {
            order_book_state->local_order_book_bids_[price] = qty;
          }
        }
      }
    }

    // Apply asks (delta: size=0 means delete, otherwise insert/update)
    if (data.contains("a") && data["a"].is_array()) {
      for (const auto& level : data["a"]) {
        if (level.is_array() && level.size() >= 2) {
          double price = std::stod(level[0].get<std::string>());
          double qty = std::stod(level[1].get<std::string>());
          if (qty == 0.0) {
            order_book_state->local_order_book_asks_.erase(price);
          } else {
            order_book_state->local_order_book_asks_[price] = qty;
          }
        }
      }
    }

    order_book_state->last_update_id_ = update_id;
    order_book_state->last_seq_ = seq;
    order_book_state->last_update_time_ = std::chrono::steady_clock::now();

    // Copy order book data for callback (while still holding lock)
    // Reuse temp_bids_/temp_asks_ buffers
    order_book_state->temp_bids_.clear();
    order_book_state->temp_asks_.clear();
    order_book_state->temp_bids_.reserve(order_book_state->local_order_book_bids_.size());
    order_book_state->temp_asks_.reserve(order_book_state->local_order_book_asks_.size());
    
    for (const auto& [price, qty] : order_book_state->local_order_book_bids_) {
      order_book_state->temp_bids_.emplace_back(price, qty);
    }
    for (const auto& [price, qty] : order_book_state->local_order_book_asks_) {
      order_book_state->temp_asks_.emplace_back(price, qty);
    }
  } // Lock released here
  
  // Call callback without holding lock to avoid blocking incoming websocket messages
  const engine::common::Instrument* instrument = FindInstrument(instrument_id);
  if (instrument) {
    SPDLOG_DEBUG("BybitHandler: Sending update to callback - {} bids, {} asks", order_book_state->temp_bids_.size(), order_book_state->temp_asks_.size());
    NotifyUpdate(*instrument, order_book_state->temp_bids_, order_book_state->temp_asks_);
  } else {
    SPDLOG_ERROR("BybitHandler: Instrument {} not found for callback", instrument_id);
  }
}

bool BybitHandler::ValidateUpdateId(InstrumentOrderBookState* order_book_state, int64_t update_id, int64_t seq) {
  if (!order_book_state) {
    return false;
  }
  
  // According to Bybit documentation:
  // - REST API `u` corresponds to 1000-level WebSocket stream `u`
  // - We subscribe to orderbook.200, so REST and WebSocket use different sequences
  // - The `u` field should be "always in sequence" for the same depth level
  // - We use WebSocket snapshot's `u` as baseline (set in ApplyWebSocketSnapshot)
  
  // If update_id is provided (> 0), validate it
  if (update_id > 0) {
    // Check: update_id >= local order book update ID
    // This validates the WebSocket sequence (orderbook.200 level)
    if (update_id < order_book_state->last_update_id_) {
      SPDLOG_DEBUG("BybitHandler: Event too old (u={} < last_update_id={}) for {}", 
                   update_id, order_book_state->last_update_id_, order_book_state->instrument_id);
      return false;  // Discard old event
    }
  }
  
  // If sequence is provided (> 0), validate it as a fallback or additional check
  // seq (cross sequence) can be used to compare different orderbook levels
  if (seq > 0) {
    // Allow sequence to be equal or greater than last_seq
    // Small gaps might be acceptable (e.g., due to network reordering)
    // Only reject if there's a significant gap (more than 1000)
    if (seq < order_book_state->last_seq_ && (order_book_state->last_seq_ - seq) > 1000) {
      SPDLOG_WARN("BybitHandler: Large sequence gap detected (seq={} < last_seq={}, gap={}) for {}", 
                  seq, order_book_state->last_seq_, order_book_state->last_seq_ - seq, order_book_state->instrument_id);
      return false;  // Large gap detected, need to reinitialize
    }
  }
  
  // If neither update_id nor seq is provided, accept the update
  // (some delta messages might not have these fields)
  if (update_id == 0 && seq == 0) {
    SPDLOG_TRACE("BybitHandler: Delta message without update_id or seq, accepting for {}", order_book_state->instrument_id);
  }

  return true;
}

void BybitHandler::ReconnectAll(engine::common::InstrumentType type) {
  auto* conn = GetOrCreateSharedConnection(type);
  if (!conn || !conn->running_.load()) {
    return;
  }

  conn->reconnect_attempt_++;
  int backoff_seconds = std::min(60, 1 << (conn->reconnect_attempt_ - 1));  // 1, 2, 4, 8, 16, 32, 60
  conn->next_reconnect_time_ = std::chrono::steady_clock::now() + std::chrono::seconds(backoff_seconds);

  std::string type_str = (type == engine::common::InstrumentType::SPOT) ? "SPOT" : "PERP";
  SPDLOG_DEBUG("BybitHandler: Scheduling {} shared connection reconnection attempt {} in {} seconds",
              type_str, conn->reconnect_attempt_, backoff_seconds);

  std::thread([this, type]() {
    auto* conn = GetOrCreateSharedConnection(type);
    if (!conn) {
      return;
    }
    
    std::this_thread::sleep_until(conn->next_reconnect_time_);
    if (conn->running_.load() && !conn->connected_.load()) {
      std::string type_str = (type == engine::common::InstrumentType::SPOT) ? "SPOT" : "PERP";
      SPDLOG_DEBUG("BybitHandler: Reconnecting {} shared connection", type_str);
      DisconnectSharedConnection(type);
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      
      // Get all subscribed instruments of this type
      std::vector<const engine::common::Instrument*> instruments;
      {
        std::lock_guard<std::mutex> lock(subscribed_mutex_);
        for (const auto& [id, inst] : subscribed_instruments_) {
          if (inst->instrument_type == type) {
            instruments.push_back(inst);
          }
        }
      }
      
      if (!instruments.empty()) {
        ConnectSharedConnectionWithRetry(type, instruments);
      }
    }
  }).detach();
}

bool BybitHandler::ParseMessage(const std::string& message,
                               std::vector<PriceLevel>& bids,
                               std::vector<PriceLevel>& asks) {
  // This method is kept for interface compatibility but not actively used
  // Parsing is handled in OnRead
  try {
    auto json = nlohmann::json::parse(message);

    if (json.contains("topic") && json.contains("type") && json.contains("data")) {
      std::string type = json["type"].get<std::string>();
      if (type == "snapshot" || type == "delta") {
        auto data = json["data"];
        
        if (data.contains("b") && data["b"].is_array()) {
          for (const auto& level : data["b"]) {
            if (level.is_array() && level.size() >= 2) {
              double price = std::stod(level[0].get<std::string>());
              double qty = std::stod(level[1].get<std::string>());
              if (qty > 0.0) {
                bids.emplace_back(price, qty);
              }
            }
          }
        }

        if (data.contains("a") && data["a"].is_array()) {
          for (const auto& level : data["a"]) {
            if (level.is_array() && level.size() >= 2) {
              double price = std::stod(level[0].get<std::string>());
              double qty = std::stod(level[1].get<std::string>());
              if (qty > 0.0) {
                asks.emplace_back(price, qty);
              }
            }
          }
        }

        return true;
      }
    }
  } catch (const std::exception& e) {
    SPDLOG_ERROR("BybitHandler: Failed to parse message: {}", e.what());
  }
  return false;
}

const engine::common::Instrument* BybitHandler::FindInstrument(const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(subscribed_mutex_);
  auto it = subscribed_instruments_.find(instrument_id);
  if (it != subscribed_instruments_.end()) {
    return it->second;
  }
  return nullptr;
}

}  // namespace herm
