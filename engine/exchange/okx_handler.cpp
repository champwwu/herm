#include "okx_handler.hpp"
#include "engine/common/util.hpp"
#include "engine/common/cpu_pinning.hpp"
#include <spdlog/spdlog.h>
#include <algorithm>
#include <csignal>
#include <future>
#include <sstream>
#include <random>

namespace herm {

OKXHandler::OKXHandler(const engine::common::ConfigManager& config)
    : MarketDataHandler("okx"),
      config_(&config),
      http_client_(std::make_unique<OKXHttpClient>()),
      rate_limiter_(std::make_unique<RateLimiter>(40, 40)) {  // Default 40 req/s
  Initialize(config);
}

OKXHandler::~OKXHandler() {
  Disconnect();
}

void OKXHandler::Initialize(const engine::common::ConfigManager& config) {
  config_ = &config;
  
  // Parse okx config section
  // Parse okx config section from market_data.okx
  auto okx_node = config.GetNodeValue("market_data.okx");
  if (okx_node.is_object()) {
    // WebSocket URL (unified for spot and perp)
    if (okx_node.contains("websocket_url") && okx_node["websocket_url"].is_string()) {
      websocket_url_ = okx_node["websocket_url"].get<std::string>();
    } else {
      websocket_url_ = "wss://ws.okx.com:8443/ws/v5/public";
    }
    
    // REST URL
    if (okx_node.contains("rest_url") && okx_node["rest_url"].is_string()) {
      rest_url_ = okx_node["rest_url"].get<std::string>();
      // Set the base URL on the HTTP client
      http_client_->SetBaseUrl(rest_url_);
    } else {
      rest_url_ = "https://www.okx.com";
    }
    
    // Rate limit
    if (okx_node.contains("rate_limit") && okx_node["rate_limit"].is_object()) {
      auto rate_limit_node = okx_node["rate_limit"];
      if (rate_limit_node.contains("requests_per_second") && rate_limit_node["requests_per_second"].is_number()) {
        rate_limit_rps_ = rate_limit_node["requests_per_second"].get<int>();
        rate_limiter_ = std::make_unique<RateLimiter>(rate_limit_rps_, rate_limit_rps_);
      }
    }
  } else {
    // Use defaults if config not found
    websocket_url_ = "wss://ws.okx.com:8443/ws/v5/public";
    rest_url_ = "https://www.okx.com";
    rate_limit_rps_ = 40;
  }
  
  SPDLOG_DEBUG("OKXHandler initialized - websocket_url: {}, rest_url: {}, rate_limit: {} req/s",
              websocket_url_, rest_url_, rate_limit_rps_);
}

bool OKXHandler::Subscribe(const engine::common::Instrument& instrument) {
  if (instrument.instrument_type != engine::common::InstrumentType::SPOT &&
      instrument.instrument_type != engine::common::InstrumentType::PERP) {
    SPDLOG_WARN("OKXHandler: Cannot subscribe non-SPOT/PERP instrument: {}", instrument.instrument_id);
    return false;
  }
  
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    
    // Check if already subscribed
    if (subscribed_instruments_.find(instrument.instrument_id) != subscribed_instruments_.end()) {
      SPDLOG_WARN("OKXHandler: Already subscribed to instrument: {}", instrument.instrument_id);
      return false;
    }
    
    // Add to subscribed instruments
    subscribed_instruments_[instrument.instrument_id] = &instrument;
  }
  
  // Create symbol mapping
  std::string exchange_symbol = GetOKXSymbol(instrument);
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
  
  SPDLOG_DEBUG("OKXHandler: Subscribed to instrument: {}", instrument.instrument_id);
  
  // Subscribe on shared connection
  return SubscribeInstrument(instrument);
}

bool OKXHandler::Unsubscribe(const std::string& instrument_id) {
  const engine::common::Instrument* instrument = nullptr;
  
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    auto it = subscribed_instruments_.find(instrument_id);
    if (it == subscribed_instruments_.end()) {
      SPDLOG_WARN("OKXHandler: Not subscribed to instrument: {}", instrument_id);
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
    std::string exchange_symbol = GetOKXSymbol(*instrument);
    std::lock_guard<std::mutex> lock(symbol_mapping_mutex_);
    symbol_to_instrument_id_.erase(exchange_symbol);
  }
  
  // Remove order book state
  {
    std::lock_guard<std::mutex> lock(order_books_mutex_);
    order_books_.erase(instrument_id);
  }
  
  SPDLOG_DEBUG("OKXHandler: Unsubscribed from instrument: {}", instrument_id);
  return true;
}

bool OKXHandler::Connect() {
  return EnsureConnected();
}

void OKXHandler::Disconnect() {
  DisconnectSharedConnection();
  
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

bool OKXHandler::IsConnected() const {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  return shared_connection_ && shared_connection_->connected_.load();
}

SharedConnection* OKXHandler::GetOrCreateSharedConnection() {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  
  if (!shared_connection_) {
    shared_connection_ = std::make_unique<SharedConnection>();
    shared_connection_->instrument_type_ = engine::common::InstrumentType::SPOT;  // OKX uses same URL for all
    shared_connection_->ws_buffer_ = std::make_shared<beast::flat_buffer>();
    shared_connection_->ws_buffer_->reserve(4096);
  }
  
  return shared_connection_.get();
}

bool OKXHandler::EnsureConnected() {
  auto* conn = GetOrCreateSharedConnection();
  if (!conn) {
    return false;
  }
  
  if (conn->connected_.load()) {
    return true;
  }
  
  // Get all subscribed instruments
  std::vector<const engine::common::Instrument*> instruments;
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    for (const auto& [id, inst] : subscribed_instruments_) {
      instruments.push_back(inst);
    }
  }
  
  if (instruments.empty()) {
    return true;  // Nothing to connect
  }
  
  return ConnectSharedConnectionWithRetry(instruments);
}

void OKXHandler::DisconnectSharedConnection() {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  
  if (shared_connection_ && shared_connection_->running_.exchange(false)) {
    if (shared_connection_->ws_ && shared_connection_->ws_->is_open()) {
      beast::error_code ec;
      shared_connection_->ws_->close(websocket::close_code::normal, ec);
    }
    if (shared_connection_->ioc_) {
      shared_connection_->ioc_->stop();
    }
    if (shared_connection_->io_thread_.joinable()) {
      shared_connection_->io_thread_.join();
    }
    shared_connection_->connected_ = false;
    SPDLOG_DEBUG("OKXHandler: Shared connection disconnected");
  }
  
  shared_connection_.reset();
}

std::string OKXHandler::GetOKXSymbol(const engine::common::Instrument& instrument) const {
  // OKX uses dash-separated symbols
  // Spot: BTC-USDT
  // Perp: BTC-USDT-SWAP
  std::string symbol = instrument.base_ccy + "-" + instrument.quote_ccy;
  if (instrument.instrument_type == engine::common::InstrumentType::PERP) {
    symbol += "-SWAP";
  }
  return symbol;
}

std::string OKXHandler::BuildSubscriptionMessage(const std::vector<const engine::common::Instrument*>& instruments) const {
  nlohmann::json sub_msg;
  sub_msg["op"] = "subscribe";
  sub_msg["args"] = nlohmann::json::array();
  
  for (const auto* inst : instruments) {
    std::string symbol = GetOKXSymbol(*inst);
    nlohmann::json arg;
    arg["channel"] = "books";
    arg["instId"] = symbol;
    sub_msg["args"].push_back(arg);
  }
  
  return sub_msg.dump();
}

std::string OKXHandler::BuildUnsubscriptionMessage(const std::string& exchange_symbol) const {
  nlohmann::json unsub_msg;
  unsub_msg["op"] = "unsubscribe";
  unsub_msg["args"] = nlohmann::json::array();
  nlohmann::json arg;
  arg["channel"] = "books";
  arg["instId"] = exchange_symbol;
  unsub_msg["args"].push_back(arg);
  return unsub_msg.dump();
}

InstrumentOrderBookState* OKXHandler::FindOrderBookBySymbol(const std::string& exchange_symbol) {
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

std::string OKXHandler::GetInstrumentIdFromSymbol(const std::string& exchange_symbol) const {
  std::lock_guard<std::mutex> lock(symbol_mapping_mutex_);
  auto it = symbol_to_instrument_id_.find(exchange_symbol);
  if (it != symbol_to_instrument_id_.end()) {
    return it->second;
  }
  return "";
}

bool OKXHandler::ConnectSharedConnection(const std::vector<const engine::common::Instrument*>& instruments) {
  auto* conn = GetOrCreateSharedConnection();
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

    // Parse WebSocket URL using utility
    auto parsed_url = engine::common::ParseWebSocketUrl(websocket_url_, "ws.okx.com", "8443");
    conn->host_ = parsed_url.host;
    conn->port_ = parsed_url.port;
    conn->path_ = parsed_url.path;

    SPDLOG_DEBUG("OKXHandler: Connecting shared connection to {}:{}{}",
                 conn->host_, conn->port_, conn->path_);

    conn->resolver_->async_resolve(conn->host_, conn->port_,
        [this, conn](beast::error_code ec, tcp::resolver::results_type results) {
          if (ec) {
            SPDLOG_ERROR("OKXHandler shared connection resolve error: {}", ec.message());
            conn->running_ = false;
            if (conn->ioc_) {
              conn->ioc_->stop();
            }
            return;
          }
          beast::get_lowest_layer(*conn->ws_).async_connect(results,
              [this, conn](beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
                if (ec) {
                  SPDLOG_ERROR("OKXHandler shared connection connect error: {}", ec.message());
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
                    [this, conn](beast::error_code ec) {
                      if (ec) {
                        SPDLOG_ERROR("OKXHandler shared connection SSL handshake error: {}", ec.message());
                        conn->running_ = false;
                        if (conn->ioc_) {
                          conn->ioc_->stop();
                        }
                        return;
                      }
                      // WebSocket handshake
                      conn->ws_->async_handshake(conn->host_, conn->path_,
                          [this, conn](beast::error_code ec) {
                            OnHandshake(conn, ec);
                          });
                    });
              });
        });

    conn->running_ = true;
    conn->io_thread_ = std::thread(&OKXHandler::RunIO, this, conn);

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
    SPDLOG_ERROR("OKXHandler shared connection error: {}", e.what());
    return false;
  }
}

bool OKXHandler::ConnectSharedConnectionWithRetry(const std::vector<const engine::common::Instrument*>& instruments, int max_retries) {
  for (int attempt = 0; attempt < max_retries; ++attempt) {
    if (attempt > 0) {
      // Exponential backoff: 1s, 2s, 4s
      int delay_ms = (1 << (attempt - 1)) * 1000;
      SPDLOG_INFO("OKXHandler: Retry attempt {}/{} for shared connection after {}ms",
                  attempt + 1, max_retries, delay_ms);
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }
    
    try {
      if (ConnectSharedConnection(instruments)) {
        SPDLOG_INFO("OKXHandler: Successfully connected shared connection on attempt {}/{}",
                    attempt + 1, max_retries);
        return true;
      }
    } catch (const std::exception& e) {
      SPDLOG_WARN("OKXHandler: Shared connection attempt {}/{} failed: {}",
                  attempt + 1, max_retries, e.what());
    }
  }
  
  SPDLOG_ERROR("OKXHandler: Failed to connect shared connection after {} attempts", max_retries);
  return false;
}

bool OKXHandler::SubscribeInstrument(const engine::common::Instrument& instrument) {
  auto* conn = GetOrCreateSharedConnection();
  if (!conn) {
    return false;
  }
  
  // Ensure connection is established
  if (!EnsureConnected()) {
    SPDLOG_WARN("OKXHandler: Failed to establish shared connection for subscribing {}", instrument.instrument_id);
    return false;
  }
  
  // Send subscription message for this instrument
  std::string exchange_symbol = GetOKXSymbol(instrument);
  nlohmann::json sub_msg;
  sub_msg["op"] = "subscribe";
  sub_msg["args"] = nlohmann::json::array();
  nlohmann::json arg;
  arg["channel"] = "books";
  arg["instId"] = exchange_symbol;
  sub_msg["args"].push_back(arg);
  
  std::string msg = sub_msg.dump();
  
  try {
    conn->ws_->write(net::buffer(msg));
    SPDLOG_DEBUG("OKXHandler: Sent subscription for {} ({})", instrument.instrument_id, exchange_symbol);
    return true;
  } catch (const std::exception& e) {
    SPDLOG_ERROR("OKXHandler: Failed to send subscription for {}: {}", instrument.instrument_id, e.what());
    return false;
  }
}

void OKXHandler::UnsubscribeInstrument(const std::string& instrument_id) {
  auto* conn = GetOrCreateSharedConnection();
  if (!conn || !conn->connected_.load()) {
    return;
  }
  
  // Find exchange symbol for this instrument
  const engine::common::Instrument* instrument = nullptr;
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    auto it = subscribed_instruments_.find(instrument_id);
    if (it != subscribed_instruments_.end()) {
      instrument = it->second;
    }
  }
  
  if (!instrument) {
    return;
  }
  
  std::string exchange_symbol = GetOKXSymbol(*instrument);
  std::string msg = BuildUnsubscriptionMessage(exchange_symbol);
  
  try {
    conn->ws_->write(net::buffer(msg));
    SPDLOG_DEBUG("OKXHandler: Sent unsubscription for {} ({})", instrument_id, exchange_symbol);
  } catch (const std::exception& e) {
    SPDLOG_ERROR("OKXHandler: Failed to send unsubscription for {}: {}", instrument_id, e.what());
  }
}

void OKXHandler::RunIO(SharedConnection* conn) {
  // Pin to CPU core if manager is set
  if (cpu_pinning_manager_) {
    try {
      cpu_pinning_manager_->PinCurrentThread("okx_io");
    } catch (const std::exception& e) {
      SPDLOG_ERROR("Failed to pin okx_io thread: {}", e.what());
      conn->running_ = false;
      if (conn->ioc_) {
        conn->ioc_->stop();
      }
      return;
    }
  }
  
  if (conn->ioc_) {
    // Run until stopped
    while (conn->running_.load() && conn->ioc_) {
      conn->ioc_->run();
      // If run() returns, check if we should continue
      if (conn->running_.load()) {
        conn->ioc_->restart();
      }
    }
  }
}

void OKXHandler::OnHandshake(SharedConnection* conn, beast::error_code ec) {
  if (ec) {
    SPDLOG_ERROR("OKXHandler: Shared connection handshake error: {}", ec.message());
    return;
  }

  conn->connected_ = true;
  SPDLOG_DEBUG("OKXHandler: Shared connection established");

  // Get all subscribed instruments
  std::vector<const engine::common::Instrument*> instruments;
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    for (const auto& [id, inst] : subscribed_instruments_) {
      instruments.push_back(inst);
    }
  }

  if (instruments.empty()) {
    SPDLOG_WARN("OKXHandler: No instruments to subscribe on shared connection");
    DoRead(conn);
    return;
  }

  // Send subscription message for all instruments
  std::string sub_msg = BuildSubscriptionMessage(instruments);
  beast::error_code write_ec;
  conn->ws_->write(net::buffer(sub_msg), write_ec);
  if (write_ec) {
    SPDLOG_ERROR("OKXHandler: Failed to send subscriptions: {}", write_ec.message());
    return;
  }
  
  SPDLOG_DEBUG("OKXHandler: Sent subscription message for {} instruments", instruments.size());

  // Get snapshots for all instruments
  for (const auto* inst : instruments) {
    GetSnapshot(*inst);
  }

  // Start reading
  DoRead(conn);
}

void OKXHandler::DoRead(SharedConnection* conn) {
  if (!conn->running_.load() || !conn->ws_ || !conn->ws_->is_open()) return;

  // Reuse pre-allocated buffer instead of creating new one
  conn->ws_buffer_->clear();
  conn->ws_->async_read(*conn->ws_buffer_,
      [this, conn](beast::error_code ec, std::size_t bytes_transferred) {
        OnRead(conn, ec, bytes_transferred, std::move(*conn->ws_buffer_));
      });
}

void OKXHandler::OnRead(SharedConnection* conn, beast::error_code ec, std::size_t bytes_transferred, beast::flat_buffer buffer) {
  // Don't process if we're shutting down
  if (!conn->running_.load()) {
    return;
  }
  
  if (ec == websocket::error::closed) {
    SPDLOG_DEBUG("OKXHandler: Shared connection WebSocket closed");
    conn->connected_ = false;
    // Trigger reconnection for all instruments
    ReconnectAll();
    return;
  }

  if (ec) {
    SPDLOG_ERROR("OKXHandler: Shared connection read error: {}", ec.message());
    // Trigger reconnection for all instruments
    ReconnectAll();
    return;
  }

  std::string message(static_cast<const char*>(buffer.data().data()), buffer.size());
  SPDLOG_DEBUG("OKXHandler: Received message ({} bytes) on shared connection", bytes_transferred);

  try {
    auto json = nlohmann::json::parse(message);

    // Handle subscription success response
    if (json.contains("event") && json.contains("arg")) {
      std::string event = json["event"].get<std::string>();
      if (event == "subscribe") {
        SPDLOG_DEBUG("OKXHandler: Subscription confirmed on shared connection");
        DoRead(conn);
        return;
      }
    }

    // Handle ping/pong
    if (json.contains("event") && json["event"] == "ping") {
      HandlePing(conn);
      DoRead(conn);
      return;
    }

    // Handle orderbook updates
    if (json.contains("arg") && json.contains("data") && json.contains("action")) {
      auto arg = json["arg"];
      if (arg.contains("channel") && arg["channel"] == "books" && arg.contains("instId")) {
        std::string exchange_symbol = arg["instId"].get<std::string>();
        
        // Find order book state for this instrument
        auto* order_book_state = FindOrderBookBySymbol(exchange_symbol);
        if (!order_book_state) {
          SPDLOG_WARN("OKXHandler: Received message for unknown instrument: {}", exchange_symbol);
          DoRead(conn);
          return;
        }
        
        if (!order_book_state->snapshot_received_) {
          // Buffer event until REST snapshot is received
          std::lock_guard<std::mutex> lock(order_books_mutex_);
          order_book_state->buffered_events_.push_back(json);
          SPDLOG_DEBUG("OKXHandler: Buffering event (REST snapshot not received yet) for {}",
                      order_book_state->instrument_id);
        } else {
          // Apply update directly
          SPDLOG_TRACE("OKXHandler: Processing orderbook update for {}",
                      order_book_state->instrument_id);
          ApplyUpdate(order_book_state, json);
        }
      }
    }
  } catch (const nlohmann::json::exception& e) {
    SPDLOG_WARN("OKXHandler: JSON parse error on shared connection: {}", e.what());
  } catch (const std::exception& e) {
    SPDLOG_WARN("OKXHandler: Parse error on shared connection: {}", e.what());
  }

  DoRead(conn);
}

void OKXHandler::HandlePing(SharedConnection* conn) {
  if (!conn->ws_ || !conn->ws_->is_open()) return;

  // OKX sends ping, we respond with pong
  nlohmann::json pong_msg;
  pong_msg["op"] = "pong";

  std::string pong_str = pong_msg.dump();
  beast::error_code ec;
  conn->ws_->write(net::buffer(pong_str), ec);
  if (ec) {
    SPDLOG_WARN("OKXHandler: Error sending pong on shared connection: {}", ec.message());
  } else {
    conn->last_ping_time_ = std::chrono::steady_clock::now();
    SPDLOG_TRACE("OKXHandler: Sent pong on shared connection");
  }
}

void OKXHandler::GetSnapshot(const engine::common::Instrument& instrument) {
  SPDLOG_DEBUG("OKXHandler: GetSnapshot called for {}", instrument.instrument_id);
  std::thread([this, inst_id = instrument.instrument_id, inst_type = instrument.instrument_type]() {
    SPDLOG_TRACE("OKXHandler: GetSnapshot thread started for {}", inst_id);
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
      SPDLOG_WARN("OKXHandler: Instrument {} no longer subscribed, skipping snapshot", inst_id);
      return;
    }

    std::string symbol = GetOKXSymbol(*instrument_ptr);
    bool is_spot = (inst_type == engine::common::InstrumentType::SPOT);
    SPDLOG_DEBUG("OKXHandler: Fetching snapshot for symbol {} ({})", symbol, inst_id);
    auto snapshot = http_client_->GetDepthSnapshot(symbol, is_spot);

    if (snapshot.lastUpdateId == 0 || snapshot.bids.empty() || snapshot.asks.empty()) {
      SPDLOG_ERROR("OKXHandler: Failed to get snapshot for {}", inst_id);
      // Retry with exponential backoff
      std::this_thread::sleep_for(std::chrono::seconds(1));
      GetSnapshot(*instrument_ptr);
      return;
    }

    SPDLOG_DEBUG("OKXHandler: Snapshot retrieved, calling ApplySnapshot for {}", inst_id);
    ApplySnapshot(inst_id, snapshot);
    SPDLOG_TRACE("OKXHandler: ApplySnapshot completed for {}", inst_id);
  }).detach();
}

void OKXHandler::ApplySnapshot(const std::string& instrument_id, const OKXHttpClient::DepthSnapshot& snapshot) {
  SPDLOG_DEBUG("OKXHandler: ApplySnapshot started for {}", instrument_id);
  
  InstrumentOrderBookState* order_book_state = nullptr;
  {
    std::lock_guard<std::mutex> lock(order_books_mutex_);
    auto it = order_books_.find(instrument_id);
    if (it == order_books_.end()) {
      SPDLOG_ERROR("OKXHandler: Order book state not found for {}", instrument_id);
      return;
    }
    order_book_state = &it->second;

    // Clear existing order book
    order_book_state->local_order_book_bids_.clear();
    order_book_state->local_order_book_asks_.clear();

    // Apply snapshot
    SPDLOG_DEBUG("OKXHandler: Applying {} bids and {} asks from snapshot", snapshot.bids.size(), snapshot.asks.size());
    for (const auto& bid : snapshot.bids) {
      order_book_state->local_order_book_bids_[bid.price] = bid.quantity;
    }
    for (const auto& ask : snapshot.asks) {
      order_book_state->local_order_book_asks_[ask.price] = ask.quantity;
    }

    order_book_state->last_update_id_ = snapshot.lastUpdateId;
    order_book_state->snapshot_received_ = true;

    SPDLOG_DEBUG("OKXHandler: Snapshot applied for {}, lastUpdateId: {}, bids: {}, asks: {}",
                instrument_id, order_book_state->last_update_id_, 
                order_book_state->local_order_book_bids_.size(), order_book_state->local_order_book_asks_.size());

    // Apply buffered events (without releasing lock)
    SPDLOG_TRACE("OKXHandler: Applying {} buffered events", order_book_state->buffered_events_.size());
    
    // Process buffered events inline to avoid recursive locking
    size_t discarded_count = 0;
    size_t applied_count = 0;
    
    for (const auto& event : order_book_state->buffered_events_) {
      if (!event.contains("data") || !event["data"].is_array() || event["data"].empty()) {
        continue;
      }
      
      // OKX data is an array, get first element
      auto data = event["data"][0];
      int64_t update_id = 0;
      
      // OKX uses "ts" (timestamp) field as update ID
      if (data.contains("ts") && data["ts"].is_string()) {
        try {
          update_id = std::stoll(data["ts"].get<std::string>());
        } catch (...) {
          update_id = 0;
        }
      }
      
      // Only apply events where update_id >= snapshot.lastUpdateId
      if (update_id < order_book_state->last_update_id_) {
        SPDLOG_DEBUG("OKXHandler: Discarding buffered event (too old): update_id={} < snapshot_id={}", 
                     update_id, order_book_state->last_update_id_);
        discarded_count++;
        continue;
      }
      
      // Apply bids inline
      if (data.contains("bids") && data["bids"].is_array()) {
        for (const auto& level : data["bids"]) {
          if (level.is_array() && level.size() >= 2) {
            std::string price_str = level[0].get<std::string>();
            std::string qty_str = level[1].get<std::string>();
            double price = std::stod(price_str);
            double qty = std::stod(qty_str);
            if (qty == 0.0) {
              order_book_state->local_order_book_bids_.erase(price);
            } else {
              order_book_state->local_order_book_bids_[price] = qty;
            }
          }
        }
      }
      
      // Apply asks inline
      if (data.contains("asks") && data["asks"].is_array()) {
        for (const auto& level : data["asks"]) {
          if (level.is_array() && level.size() >= 2) {
            std::string price_str = level[0].get<std::string>();
            std::string qty_str = level[1].get<std::string>();
            double price = std::stod(price_str);
            double qty = std::stod(qty_str);
            if (qty == 0.0) {
              order_book_state->local_order_book_asks_.erase(price);
            } else {
              order_book_state->local_order_book_asks_[price] = qty;
            }
          }
        }
      }
      
      order_book_state->last_update_id_ = update_id;
      applied_count++;
    }
    
    if (discarded_count > 0) {
      SPDLOG_DEBUG("OKXHandler: Applied {}/{} buffered events ({} discarded as stale)", 
                  applied_count, order_book_state->buffered_events_.size(), discarded_count);
    }
    
    order_book_state->buffered_events_.clear();

    // Copy order book data for callback (while still holding lock)
    // Reuse temp_bids_/temp_asks_ buffers
    order_book_state->temp_bids_.clear();
    order_book_state->temp_asks_.clear();
    order_book_state->temp_bids_.reserve(order_book_state->local_order_book_bids_.size());
    order_book_state->temp_asks_.reserve(order_book_state->local_order_book_asks_.size());
    
    SPDLOG_TRACE("OKXHandler: Preparing to send initial order book to callback");
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
    SPDLOG_TRACE("OKXHandler: Calling NotifyUpdate with {} bids, {} asks", order_book_state->temp_bids_.size(), order_book_state->temp_asks_.size());
    NotifyUpdate(*instrument, order_book_state->temp_bids_, order_book_state->temp_asks_);
    SPDLOG_TRACE("OKXHandler: NotifyUpdate completed, ApplySnapshot finished");
  } else {
    SPDLOG_ERROR("OKXHandler: Instrument {} not found for callback", instrument_id);
  }
}

void OKXHandler::ApplyUpdate(InstrumentOrderBookState* order_book_state, const nlohmann::json& event) {
  if (!order_book_state) {
    return;
  }
  
  std::string instrument_id = order_book_state->instrument_id;
  
  {
    std::lock_guard<std::mutex> lock(order_books_mutex_);

    if (!order_book_state->snapshot_received_) {
      return;
    }

    if (!event.contains("data") || !event["data"].is_array() || event["data"].empty()) {
      return;
    }

    // OKX data is an array, get first element
    auto data = event["data"][0];
    
    // Extract update ID (timestamp)
    int64_t update_id = 0;
    if (data.contains("ts") && data["ts"].is_string()) {
      try {
        update_id = std::stoll(data["ts"].get<std::string>());
      } catch (...) {
        update_id = 0;
      }
    }

    // Validate update ID
    if (!ValidateUpdateId(order_book_state, update_id)) {
      SPDLOG_WARN("OKXHandler: Update ID validation failed for {}, reinitializing order book", instrument_id);
      order_book_state->snapshot_received_ = false;
      order_book_state->buffered_events_.clear();
      const engine::common::Instrument* instrument = FindInstrument(instrument_id);
      if (instrument) {
        GetSnapshot(*instrument);
      }
      return;
    }

    // Apply bids (OKX uses "bids" array with [price, size, ...] format)
    if (data.contains("bids") && data["bids"].is_array()) {
      for (const auto& level : data["bids"]) {
        if (level.is_array() && level.size() >= 2) {
          std::string price_str = level[0].get<std::string>();
          std::string qty_str = level[1].get<std::string>();
          double price = std::stod(price_str);
          double qty = std::stod(qty_str);
          if (qty == 0.0) {
            order_book_state->local_order_book_bids_.erase(price);
          } else {
            order_book_state->local_order_book_bids_[price] = qty;
          }
        }
      }
    }

    // Apply asks
    if (data.contains("asks") && data["asks"].is_array()) {
      for (const auto& level : data["asks"]) {
        if (level.is_array() && level.size() >= 2) {
          std::string price_str = level[0].get<std::string>();
          std::string qty_str = level[1].get<std::string>();
          double price = std::stod(price_str);
          double qty = std::stod(qty_str);
          if (qty == 0.0) {
            order_book_state->local_order_book_asks_.erase(price);
          } else {
            order_book_state->local_order_book_asks_[price] = qty;
          }
        }
      }
    }

    order_book_state->last_update_id_ = update_id;
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
    SPDLOG_DEBUG("OKXHandler: Sending update to callback - {} bids, {} asks", order_book_state->temp_bids_.size(), order_book_state->temp_asks_.size());
    NotifyUpdate(*instrument, order_book_state->temp_bids_, order_book_state->temp_asks_);
  } else {
    SPDLOG_ERROR("OKXHandler: Instrument {} not found for callback", instrument_id);
  }
}

bool OKXHandler::ValidateUpdateId(InstrumentOrderBookState* order_book_state, int64_t update_id) {
  if (!order_book_state) {
    return false;
  }
  
  // Check: update_id >= local order book update ID
  if (update_id < order_book_state->last_update_id_) {
    SPDLOG_DEBUG("OKXHandler: Event too old (ts={} < last_update_id={}) for {}", 
                 update_id, order_book_state->last_update_id_, order_book_state->instrument_id);
    return false;  // Discard old event
  }

  return true;
}

void OKXHandler::ReconnectAll() {
  auto* conn = GetOrCreateSharedConnection();
  if (!conn || !conn->running_.load()) {
    return;
  }

  conn->reconnect_attempt_++;
  int backoff_seconds = std::min(60, 1 << (conn->reconnect_attempt_ - 1));  // 1, 2, 4, 8, 16, 32, 60
  conn->next_reconnect_time_ = std::chrono::steady_clock::now() + std::chrono::seconds(backoff_seconds);

  SPDLOG_DEBUG("OKXHandler: Scheduling shared connection reconnection attempt {} in {} seconds",
              conn->reconnect_attempt_, backoff_seconds);

  std::thread([this]() {
    auto* conn = GetOrCreateSharedConnection();
    if (!conn) {
      return;
    }
    
    std::this_thread::sleep_until(conn->next_reconnect_time_);
    if (conn->running_.load() && !conn->connected_.load()) {
      SPDLOG_DEBUG("OKXHandler: Reconnecting shared connection");
      DisconnectSharedConnection();
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      
      // Get all subscribed instruments
      std::vector<const engine::common::Instrument*> instruments;
      {
        std::lock_guard<std::mutex> lock(subscribed_mutex_);
        for (const auto& [id, inst] : subscribed_instruments_) {
          instruments.push_back(inst);
        }
      }
      
      if (!instruments.empty()) {
        ConnectSharedConnectionWithRetry(instruments);
      }
    }
  }).detach();
}

bool OKXHandler::ParseMessage(const std::string& message,
                              std::vector<PriceLevel>& bids,
                              std::vector<PriceLevel>& asks) {
  // This method is kept for interface compatibility but not actively used
  // Parsing is handled in OnRead
  try {
    auto json = nlohmann::json::parse(message);

    if (json.contains("data") && json["data"].is_array() && !json["data"].empty()) {
      auto data = json["data"][0];
      
      if (data.contains("bids") && data["bids"].is_array()) {
        for (const auto& level : data["bids"]) {
          if (level.is_array() && level.size() >= 2) {
            std::string price_str = level[0].get<std::string>();
            std::string qty_str = level[1].get<std::string>();
            double price = std::stod(price_str);
            double qty = std::stod(qty_str);
            if (qty > 0.0) {
              bids.emplace_back(price, qty);
            }
          }
        }
      }

      if (data.contains("asks") && data["asks"].is_array()) {
        for (const auto& level : data["asks"]) {
          if (level.is_array() && level.size() >= 2) {
            std::string price_str = level[0].get<std::string>();
            std::string qty_str = level[1].get<std::string>();
            double price = std::stod(price_str);
            double qty = std::stod(qty_str);
            if (qty > 0.0) {
              asks.emplace_back(price, qty);
            }
          }
        }
      }

      return true;
    }
  } catch (const std::exception& e) {
    SPDLOG_ERROR("OKXHandler: Failed to parse message: {}", e.what());
  }
  return false;
}

const engine::common::Instrument* OKXHandler::FindInstrument(const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(subscribed_mutex_);
  auto it = subscribed_instruments_.find(instrument_id);
  if (it != subscribed_instruments_.end()) {
    return it->second;
  }
  return nullptr;
}

}  // namespace herm
