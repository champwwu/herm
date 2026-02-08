#include "binance_futures_handler.hpp"
#include "engine/common/util.hpp"
#include "engine/common/cpu_pinning.hpp"
#include <spdlog/spdlog.h>
#include <algorithm>
#include <csignal>
#include <future>
#include <sstream>
#include <random>

namespace herm {

BinanceFuturesHandler::BinanceFuturesHandler(const engine::common::ConfigManager& config)
    : MarketDataHandler("binance"),
      config_(&config),
      http_client_(std::make_unique<BinanceHttpClient>()),
      rate_limiter_(std::make_unique<RateLimiter>(20, 20)) {  // Default 20 req/s, will be updated from config
  Initialize(config);
}

BinanceFuturesHandler::~BinanceFuturesHandler() {
  // Disconnect() should have been called explicitly before destruction
  // Calling it here as safety fallback
  if (shared_connection_) {
    SPDLOG_WARN("BinanceFuturesHandler: Connection not empty in destructor, calling Disconnect()");
    Disconnect();
  }
}

void BinanceFuturesHandler::Initialize(const engine::common::ConfigManager& config) {
  config_ = &config;
  
  // Parse binance config section from market_data.binance
  auto binance_node = config.GetNodeValue("market_data.binance");
  if (binance_node.is_object() && binance_node.contains("futures")) {
    auto futures_node = binance_node["futures"];
    
    if (futures_node.contains("websocket_url_template") && futures_node["websocket_url_template"].is_string()) {
      websocket_url_template_ = futures_node["websocket_url_template"].get<std::string>();
    } else {
      // Default template
      websocket_url_template_ = "wss://fstream.binance.com:443/ws/{symbol}@depth@100ms";
    }
    
    if (futures_node.contains("rest_url") && futures_node["rest_url"].is_string()) {
      rest_url_ = futures_node["rest_url"].get<std::string>();
    } else {
      rest_url_ = "https://fapi.binance.com";
    }
    
    if (futures_node.contains("rate_limit") && futures_node["rate_limit"].is_object()) {
      auto rate_limit_node = futures_node["rate_limit"];
      if (rate_limit_node.contains("requests_per_second") && rate_limit_node["requests_per_second"].is_number()) {
        rate_limit_rps_ = rate_limit_node["requests_per_second"].get<int>();
        rate_limiter_ = std::make_unique<RateLimiter>(rate_limit_rps_, rate_limit_rps_);
      }
    }
  } else {
    // Use defaults if config not found
    websocket_url_template_ = "wss://fstream.binance.com:443/ws/{symbol}@depth@100ms";
    rest_url_ = "https://fapi.binance.com";
    rate_limit_rps_ = 20;
  }
  
  SPDLOG_DEBUG("BinanceFuturesHandler initialized - websocket_template: {}, rest_url: {}, rate_limit: {} req/s",
              websocket_url_template_, rest_url_, rate_limit_rps_);
}

bool BinanceFuturesHandler::Subscribe(const engine::common::Instrument& instrument) {
  if (instrument.instrument_type != engine::common::InstrumentType::PERP) {
    SPDLOG_WARN("BinanceFuturesHandler: Cannot subscribe non-PERP instrument: {}", instrument.instrument_id);
    return false;
  }
  
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    
    // Check if already subscribed
    if (subscribed_instruments_.find(instrument.instrument_id) != subscribed_instruments_.end()) {
      SPDLOG_WARN("BinanceFuturesHandler: Already subscribed to instrument: {}", instrument.instrument_id);
      return false;
    }
    
    // Add to subscribed instruments
    subscribed_instruments_[instrument.instrument_id] = &instrument;
  }
  
  // Create stream mapping
  std::string stream = GetStreamName(instrument);
  {
    std::lock_guard<std::mutex> lock(stream_mapping_mutex_);
    stream_to_instrument_id_[stream] = instrument.instrument_id;
  }
  
  // Initialize order book state
  {
    std::lock_guard<std::mutex> lock(order_books_mutex_);
    auto& order_book_state = order_books_[instrument.instrument_id];
    order_book_state.instrument_id = instrument.instrument_id;
    order_book_state.buffered_events_.reserve(32);
    order_book_state.temp_valid_events_.reserve(32);
  }
  
  SPDLOG_DEBUG("BinanceFuturesHandler: Subscribed to instrument: {}", instrument.instrument_id);
  
  // Subscribe on shared connection (will reconnect if needed)
  return SubscribeInstrument(instrument);
}

bool BinanceFuturesHandler::Unsubscribe(const std::string& instrument_id) {
  const engine::common::Instrument* instrument = nullptr;
  
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    auto it = subscribed_instruments_.find(instrument_id);
    if (it == subscribed_instruments_.end()) {
      SPDLOG_WARN("BinanceFuturesHandler: Not subscribed to instrument: {}", instrument_id);
      return false;
    }
    instrument = it->second;
    subscribed_instruments_.erase(it);
  }
  
  // Unsubscribe from shared connection (will reconnect with remaining instruments)
  if (instrument) {
    UnsubscribeInstrument(instrument_id);
  }
  
  // Remove stream mapping
  if (instrument) {
    std::string stream = GetStreamName(*instrument);
    std::lock_guard<std::mutex> lock(stream_mapping_mutex_);
    stream_to_instrument_id_.erase(stream);
  }
  
  // Remove order book state
  {
    std::lock_guard<std::mutex> lock(order_books_mutex_);
    order_books_.erase(instrument_id);
  }
  
  SPDLOG_DEBUG("BinanceFuturesHandler: Unsubscribed from instrument: {}", instrument_id);
  return true;
}

bool BinanceFuturesHandler::Connect() {
  return EnsureConnected();
}

void BinanceFuturesHandler::Disconnect() {
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
    std::lock_guard<std::mutex> lock(stream_mapping_mutex_);
    stream_to_instrument_id_.clear();
  }
}

bool BinanceFuturesHandler::IsConnected() const {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  return shared_connection_ && shared_connection_->connected_.load();
}

SharedConnection* BinanceFuturesHandler::GetOrCreateSharedConnection() {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  
  if (!shared_connection_) {
    shared_connection_ = std::make_unique<SharedConnection>();
    shared_connection_->instrument_type_ = engine::common::InstrumentType::PERP;
    shared_connection_->ws_buffer_ = std::make_shared<beast::flat_buffer>();
    shared_connection_->ws_buffer_->reserve(4096);
  }
  
  return shared_connection_.get();
}

bool BinanceFuturesHandler::EnsureConnected() {
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

void BinanceFuturesHandler::DisconnectSharedConnection() {
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
    SPDLOG_DEBUG("BinanceFuturesHandler: Shared connection disconnected");
  }
  
  shared_connection_.reset();
}

std::string BinanceFuturesHandler::GetBinanceSymbol(const engine::common::Instrument& instrument) const {
  std::string symbol = instrument.GetSymbol();  // Returns "BTCUSDT"
  std::transform(symbol.begin(), symbol.end(), symbol.begin(), ::tolower);
  return symbol;  // Returns "btcusdt"
}

std::string BinanceFuturesHandler::GetStreamName(const engine::common::Instrument& instrument) const {
  // Binance stream format: {symbol}@depth (e.g., "btcusdt@depth")
  return GetBinanceSymbol(instrument) + "@depth";
}

InstrumentOrderBookState* BinanceFuturesHandler::FindOrderBookByStream(const std::string& stream) {
  // Find instrument ID from stream
  std::string instrument_id;
  {
    std::lock_guard<std::mutex> lock(stream_mapping_mutex_);
    auto it = stream_to_instrument_id_.find(stream);
    if (it == stream_to_instrument_id_.end()) {
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

std::string BinanceFuturesHandler::GetInstrumentIdFromStream(const std::string& stream) const {
  std::lock_guard<std::mutex> lock(stream_mapping_mutex_);
  auto it = stream_to_instrument_id_.find(stream);
  if (it != stream_to_instrument_id_.end()) {
    return it->second;
  }
  return "";
}

std::string BinanceFuturesHandler::BuildCombinedStreamUrl(const std::vector<const engine::common::Instrument*>& instruments) const {
  if (instruments.empty()) {
    return "";
  }
  
  // Extract base URL from websocket_url_template_ (remove /ws/{symbol}@depth@100ms part)
  // e.g., "wss://localhost:8091/ws/{symbol}@depth@100ms" -> "wss://localhost:8091"
  std::string base_url = websocket_url_template_;
  size_t ws_pos = base_url.find("/ws/");
  if (ws_pos != std::string::npos) {
    base_url = base_url.substr(0, ws_pos);
  }
  
  // Build combined stream URL: {base_url}/stream?streams=stream1/stream2/stream3
  base_url += "/stream?streams=";
  
  for (size_t i = 0; i < instruments.size(); ++i) {
    if (i > 0) {
      base_url += "/";
    }
    base_url += GetStreamName(*instruments[i]);
  }
  
  return base_url;
}

bool BinanceFuturesHandler::SubscribeInstrument(const engine::common::Instrument& instrument) {
  // For Binance, we need to reconnect with all streams in the URL
  // So subscription means triggering a reconnection with the updated stream list
  return EnsureConnected();
}

void BinanceFuturesHandler::UnsubscribeInstrument(const std::string& instrument_id) {
  // For Binance, we need to reconnect with remaining streams
  // Disconnect and reconnect with remaining instruments
  DisconnectSharedConnection();
  
  // Reconnect with remaining instruments if any exist
  std::vector<const engine::common::Instrument*> remaining_instruments;
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    for (const auto& [id, inst] : subscribed_instruments_) {
      remaining_instruments.push_back(inst);
    }
  }
  
  if (!remaining_instruments.empty()) {
    ConnectSharedConnectionWithRetry(remaining_instruments);
  }
}

bool BinanceFuturesHandler::ConnectSharedConnection(const std::vector<const engine::common::Instrument*>& instruments) {
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

    // Build combined stream URL
    std::string url = BuildCombinedStreamUrl(instruments);
    
    // Parse URL using utility (extract default host/port from websocket_url_template_)
    std::string default_host = "fstream.binance.com";
    std::string default_port = "443";
    
    // Try to extract host and port from websocket_url_template_ if it's not production
    if (websocket_url_template_.find("localhost") != std::string::npos) {
      auto template_parsed = engine::common::ParseWebSocketUrl(websocket_url_template_, "localhost", "8091");
      default_host = template_parsed.host;
      default_port = template_parsed.port;
    }
    
    auto parsed_url = engine::common::ParseWebSocketUrl(url, default_host, default_port);
    conn->host_ = parsed_url.host;
    conn->port_ = parsed_url.port;
    conn->path_ = parsed_url.path;

    SPDLOG_DEBUG("BinanceFuturesHandler: Connecting shared connection to {}:{}{}",
                 conn->host_, conn->port_, conn->path_);

    conn->resolver_->async_resolve(conn->host_, conn->port_,
        [this, conn](beast::error_code ec, tcp::resolver::results_type results) {
          if (ec) {
            SPDLOG_ERROR("BinanceFuturesHandler shared connection resolve error: {}", ec.message());
            conn->running_ = false;
            if (conn->ioc_) {
              conn->ioc_->stop();
            }
            return;
          }
          beast::get_lowest_layer(*conn->ws_).async_connect(results,
              [this, conn](beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
                if (ec) {
                  SPDLOG_ERROR("BinanceFuturesHandler shared connection connect error: {}", ec.message());
                  conn->running_ = false;
                  if (conn->ioc_) {
                    conn->ioc_->stop();
                  }
                  return;
                }
                // SSL handshake
                conn->ws_->next_layer().async_handshake(ssl::stream_base::client,
                    [this, conn](beast::error_code ec) {
                      if (ec) {
                        SPDLOG_ERROR("BinanceFuturesHandler shared connection SSL handshake error: {}", ec.message());
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
    conn->io_thread_ = std::thread(&BinanceFuturesHandler::RunIO, this, conn);

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
    SPDLOG_ERROR("BinanceFuturesHandler shared connection error: {}", e.what());
    return false;
  }
}

bool BinanceFuturesHandler::ConnectSharedConnectionWithRetry(const std::vector<const engine::common::Instrument*>& instruments, int max_retries) {
  for (int attempt = 0; attempt < max_retries; ++attempt) {
    if (attempt > 0) {
      // Exponential backoff: 1s, 2s, 4s
      int delay_ms = (1 << (attempt - 1)) * 1000;
      SPDLOG_INFO("BinanceFuturesHandler: Retry attempt {}/{} for shared connection after {}ms",
                  attempt + 1, max_retries, delay_ms);
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }
    
    try {
      if (ConnectSharedConnection(instruments)) {
        SPDLOG_INFO("BinanceFuturesHandler: Successfully connected shared connection on attempt {}/{}",
                    attempt + 1, max_retries);
        return true;
      }
    } catch (const std::exception& e) {
      SPDLOG_WARN("BinanceFuturesHandler: Shared connection attempt {}/{} failed: {}",
                  attempt + 1, max_retries, e.what());
    }
  }
  
  SPDLOG_ERROR("BinanceFuturesHandler: Failed to connect shared connection after {} attempts", max_retries);
  return false;
}

void BinanceFuturesHandler::RunIO(SharedConnection* conn) {
  // Pin to CPU core if manager is set
  if (cpu_pinning_manager_) {
    try {
      cpu_pinning_manager_->PinCurrentThread("binance_futures_io");
    } catch (const std::exception& e) {
      SPDLOG_ERROR("Failed to pin binance_futures_io thread: {}", e.what());
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

void BinanceFuturesHandler::OnHandshake(SharedConnection* conn, beast::error_code ec) {
  if (ec) {
    SPDLOG_ERROR("BinanceFuturesHandler: Shared connection handshake error: {}", ec.message());
    return;
  }

  conn->connected_ = true;
  SPDLOG_DEBUG("BinanceFuturesHandler: Shared connection established");

  // Get all subscribed instruments
  std::vector<const engine::common::Instrument*> instruments;
  {
    std::lock_guard<std::mutex> lock(subscribed_mutex_);
    for (const auto& [id, inst] : subscribed_instruments_) {
      instruments.push_back(inst);
    }
  }

  if (instruments.empty()) {
    SPDLOG_WARN("BinanceFuturesHandler: No instruments to initialize on shared connection");
    DoRead(conn);
    return;
  }

  // Get snapshots for all instruments
  for (const auto* inst : instruments) {
    GetSnapshot(*inst);
  }

  // Start reading
  DoRead(conn);
}

void BinanceFuturesHandler::DoRead(SharedConnection* conn) {
  if (!conn->running_.load() || !conn->ws_ || !conn->ws_->is_open()) return;

  // Reuse pre-allocated buffer instead of creating new one
  conn->ws_buffer_->clear();
  conn->ws_->async_read(*conn->ws_buffer_,
      [this, conn](beast::error_code ec, std::size_t bytes_transferred) {
        OnRead(conn, ec, bytes_transferred, std::move(*conn->ws_buffer_));
      });
}

void BinanceFuturesHandler::OnRead(SharedConnection* conn, beast::error_code ec, std::size_t bytes_transferred, beast::flat_buffer buffer) {
  // Don't process if we're shutting down
  if (!conn->running_.load()) {
    return;
  }
  
  if (ec == websocket::error::closed) {
    SPDLOG_DEBUG("BinanceFuturesHandler: Shared connection WebSocket closed");
    conn->connected_ = false;
    // Trigger reconnection
    Reconnect(conn);
    return;
  }

  if (ec) {
    SPDLOG_ERROR("BinanceFuturesHandler: Shared connection read error: {}", ec.message());
    // Trigger reconnection
    Reconnect(conn);
    return;
  }

  std::string message(static_cast<const char*>(buffer.data().data()), buffer.size());
  SPDLOG_TRACE("BinanceFuturesHandler: Received message ({} bytes)", bytes_transferred);

  try {
    auto json = nlohmann::json::parse(message);

    // Handle combined stream message format: {"stream":"<streamName>","data":{...}}
    if (json.contains("stream") && json.contains("data")) {
      std::string stream_name = json["stream"].get<std::string>();
      auto data = json["data"];
      
      // Route to correct order book by stream name
      InstrumentOrderBookState* ob_state = FindOrderBookByStream(stream_name);
      if (!ob_state) {
        SPDLOG_WARN("BinanceFuturesHandler: Received message for unknown stream: {}", stream_name);
        DoRead(conn);
        return;
      }

      // Handle ping/pong (Binance may send pings in data)
      if (data.contains("ping")) {
        HandlePing(conn, data["ping"].get<std::string>());
        DoRead(conn);
        return;
      }

      // Handle depthUpdate
      if (data.contains("e") && data["e"] == "depthUpdate") {
        if (!ob_state->snapshot_received_) {
          // Buffer event until snapshot is received
          std::lock_guard<std::mutex> lock(ob_state->order_book_mutex_);
          ob_state->buffered_events_.push_back(data);
          SPDLOG_TRACE("BinanceFuturesHandler: Buffering event (snapshot not received yet) for {}", ob_state->instrument_id);
        } else {
          // Apply update directly
          SPDLOG_TRACE("BinanceFuturesHandler: Processing depthUpdate event for {}", ob_state->instrument_id);
          ApplyUpdate(ob_state, data);
        }
      } else {
        SPDLOG_DEBUG("BinanceFuturesHandler: Received non-depthUpdate message for {}: {}", ob_state->instrument_id, message.substr(0, 100));
      }
    } else {
      SPDLOG_DEBUG("BinanceFuturesHandler: Received non-stream message: {}", message.substr(0, 100));
    }
  } catch (const nlohmann::json::exception& e) {
    SPDLOG_WARN("BinanceFuturesHandler: JSON parse error: {}", e.what());
  } catch (const std::exception& e) {
    SPDLOG_WARN("BinanceFuturesHandler: Parse error: {}", e.what());
  }

  DoRead(conn);
}

void BinanceFuturesHandler::HandlePing(SharedConnection* conn, const std::string& payload) {
  if (!conn->ws_ || !conn->ws_->is_open()) return;

  nlohmann::json pong_msg;
  pong_msg["pong"] = payload;

  std::string pong_str = pong_msg.dump();
  beast::error_code ec;
  conn->ws_->write(net::buffer(pong_str), ec);
  if (ec) {
    SPDLOG_WARN("BinanceFuturesHandler: Error sending pong: {}", ec.message());
  } else {
    conn->last_ping_time_ = std::chrono::steady_clock::now();
    SPDLOG_TRACE("BinanceFuturesHandler: Sent pong");
  }
}

void BinanceFuturesHandler::GetSnapshot(const engine::common::Instrument& instrument) {
  SPDLOG_DEBUG("BinanceFuturesHandler: GetSnapshot called for {}", instrument.instrument_id);
  // Copy instrument data to avoid use-after-free in detached thread
  std::string instrument_id = instrument.instrument_id;
  std::string symbol = instrument.GetSymbol();
  
  std::thread([this, instrument_id, symbol]() {
    SPDLOG_TRACE("BinanceFuturesHandler: GetSnapshot thread started for {}", instrument_id);
    // Add small random delay (0-500ms) to stagger snapshot requests from multiple instruments
    // This helps avoid hitting rate limits when multiple instruments subscribe simultaneously
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 500);
    std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));
    
    // Rate limit
    rate_limiter_->Acquire();

    // For REST API, use uppercase symbol
    SPDLOG_DEBUG("BinanceFuturesHandler: Fetching snapshot for symbol {} ({})", symbol, instrument_id);
    auto snapshot = http_client_->GetDepthSnapshot(symbol, true);  // true = futures

    if (snapshot.lastUpdateId == 0 || snapshot.bids.empty() || snapshot.asks.empty()) {
      SPDLOG_ERROR("BinanceFuturesHandler: Failed to get snapshot for {}", instrument_id);
      // Retry with exponential backoff
      std::this_thread::sleep_for(std::chrono::seconds(1));
      const engine::common::Instrument* inst = FindInstrument(instrument_id);
      if (inst) {
        GetSnapshot(*inst);
      }
      return;
    }

    SPDLOG_DEBUG("BinanceFuturesHandler: Snapshot retrieved, calling ApplySnapshot for {}", instrument_id);
    ApplySnapshot(instrument_id, snapshot);
    SPDLOG_TRACE("BinanceFuturesHandler: ApplySnapshot completed for {}", instrument_id);
  }).detach();
}

void BinanceFuturesHandler::ApplySnapshot(const std::string& instrument_id, const BinanceHttpClient::DepthSnapshot& snapshot) {
  SPDLOG_DEBUG("BinanceFuturesHandler: ApplySnapshot started for {}", instrument_id);
  
  // Find order book state
  InstrumentOrderBookState* ob_state = nullptr;
  {
    std::lock_guard<std::mutex> lock(order_books_mutex_);
    auto it = order_books_.find(instrument_id);
    if (it == order_books_.end()) {
      SPDLOG_ERROR("BinanceFuturesHandler: Order book state not found for {}", instrument_id);
      return;
    }
    ob_state = &it->second;
  }
  
  {
    std::lock_guard<std::mutex> lock(ob_state->order_book_mutex_);

    // Clear existing order book
    ob_state->local_order_book_bids_.clear();
    ob_state->local_order_book_asks_.clear();

    // Apply snapshot
    SPDLOG_DEBUG("BinanceFuturesHandler: Applying {} bids and {} asks from snapshot", snapshot.bids.size(), snapshot.asks.size());
    for (const auto& bid : snapshot.bids) {
      ob_state->local_order_book_bids_[bid.price] = bid.quantity;
    }
    for (const auto& ask : snapshot.asks) {
      ob_state->local_order_book_asks_[ask.price] = ask.quantity;
    }

    ob_state->last_update_id_ = snapshot.lastUpdateId;
    ob_state->snapshot_received_ = true;

    SPDLOG_DEBUG("BinanceFuturesHandler: Snapshot applied for {}, lastUpdateId: {}, bids: {}, asks: {}",
                instrument_id, ob_state->last_update_id_, 
                ob_state->local_order_book_bids_.size(), ob_state->local_order_book_asks_.size());

    // Apply buffered events (without releasing lock)
    SPDLOG_TRACE("BinanceFuturesHandler: Applying {} buffered events", ob_state->buffered_events_.size());
    
    // Filter buffered events based on exchange-specific validation logic
    FilterBufferedEvents(ob_state);
    
    // Apply valid buffered events to order book
    ApplyBufferedEventsToOrderBook(ob_state);
    
    ob_state->buffered_events_.clear();

    // Prepare callback data
    PrepareCallbackData(ob_state);
  } // Lock released here
  
  // Call callback without holding lock to avoid blocking incoming websocket messages
  const engine::common::Instrument* instrument = FindInstrument(instrument_id);
  if (instrument) {
    SPDLOG_TRACE("BinanceFuturesHandler: Calling NotifyUpdate with {} bids, {} asks", ob_state->temp_bids_.size(), ob_state->temp_asks_.size());
    NotifyUpdate(*instrument, ob_state->temp_bids_, ob_state->temp_asks_);
    SPDLOG_TRACE("BinanceFuturesHandler: NotifyUpdate completed, ApplySnapshot finished");
  } else {
    SPDLOG_ERROR("BinanceFuturesHandler: Instrument {} not found for callback", instrument_id);
  }
}

void BinanceFuturesHandler::ApplyUpdate(InstrumentOrderBookState* ob_state, const nlohmann::json& event) {
  {
    std::lock_guard<std::mutex> lock(ob_state->order_book_mutex_);

    if (!ob_state->snapshot_received_) {
      return;
    }

    // Extract update IDs
    int64_t first_update_id = event["U"].get<int64_t>();
    int64_t final_update_id = event["u"].get<int64_t>();
    int64_t prev_id = event.contains("pu") ? event["pu"].get<int64_t>() : 0;

    // Validate update IDs
    if (!ValidateUpdateId(ob_state, first_update_id, final_update_id, prev_id)) {
      SPDLOG_WARN("BinanceFuturesHandler: Update ID validation failed for {}, reinitializing order book", ob_state->instrument_id);
      ob_state->snapshot_received_ = false;
      ob_state->buffered_events_.clear();
      const engine::common::Instrument* instrument = FindInstrument(ob_state->instrument_id);
      if (instrument) {
        GetSnapshot(*instrument);
      }
      return;
    }

    // Apply bids
    if (event.contains("b") && event["b"].is_array()) {
      for (const auto& level : event["b"]) {
        if (level.is_array() && level.size() >= 2) {
          double price = std::stod(level[0].get<std::string>());
          double qty = std::stod(level[1].get<std::string>());
          if (qty == 0.0) {
            ob_state->local_order_book_bids_.erase(price);
          } else {
            ob_state->local_order_book_bids_[price] = qty;
          }
        }
      }
    }

    // Apply asks
    if (event.contains("a") && event["a"].is_array()) {
      for (const auto& level : event["a"]) {
        if (level.is_array() && level.size() >= 2) {
          double price = std::stod(level[0].get<std::string>());
          double qty = std::stod(level[1].get<std::string>());
          if (qty == 0.0) {
            ob_state->local_order_book_asks_.erase(price);
          } else {
            ob_state->local_order_book_asks_[price] = qty;
          }
        }
      }
    }

    ob_state->last_update_id_ = final_update_id;
    ob_state->last_update_time_ = std::chrono::steady_clock::now();

    // Copy order book data for callback (while still holding lock)
    // Reuse temp_bids_/temp_asks_ buffers
    ob_state->temp_bids_.clear();
    ob_state->temp_asks_.clear();
    ob_state->temp_bids_.reserve(ob_state->local_order_book_bids_.size());
    ob_state->temp_asks_.reserve(ob_state->local_order_book_asks_.size());
    
    for (const auto& [price, qty] : ob_state->local_order_book_bids_) {
      ob_state->temp_bids_.emplace_back(price, qty);
    }
    for (const auto& [price, qty] : ob_state->local_order_book_asks_) {
      ob_state->temp_asks_.emplace_back(price, qty);
    }
  } // Lock released here
  
  // Call callback without holding lock to avoid blocking incoming websocket messages
  const engine::common::Instrument* instrument = FindInstrument(ob_state->instrument_id);
  if (instrument) {
    SPDLOG_DEBUG("BinanceFuturesHandler: Sending update to callback - {} bids, {} asks", ob_state->temp_bids_.size(), ob_state->temp_asks_.size());
    NotifyUpdate(*instrument, ob_state->temp_bids_, ob_state->temp_asks_);
  } else {
    SPDLOG_ERROR("BinanceFuturesHandler: Instrument {} not found for callback", ob_state->instrument_id);
  }
}

bool BinanceFuturesHandler::ValidateUpdateId(InstrumentOrderBookState* ob_state, int64_t first_id, int64_t final_id, int64_t prev_id) {
  // Binance Futures uses "pu" (previous update ID) for continuity validation
  // Reference: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
  
  // Check: u (final update ID) > local order book update ID (must be newer)
  if (final_id <= ob_state->last_update_id_) {
    SPDLOG_DEBUG("BinanceFuturesHandler: Event too old or duplicate (u={} <= last_update_id={}) for {}", 
                 final_id, ob_state->last_update_id_, ob_state->instrument_id);
    return false;  // Discard old/duplicate event
  }

  // For Futures: Check pu (previous update ID) == last_update_id for continuity
  // This is the key difference from Spot according to Binance documentation
  if (prev_id > 0 && prev_id != ob_state->last_update_id_) {
    SPDLOG_WARN("BinanceFuturesHandler: Gap detected (pu={} != last_update_id={}) for {}", 
                prev_id, ob_state->last_update_id_, ob_state->instrument_id);
    return false;  // Gap detected, need to reinitialize
  }

  return true;
}

void BinanceFuturesHandler::FilterBufferedEvents(InstrumentOrderBookState* ob_state) {
  // Filter buffered events: Apply Binance Futures logic for snapshot alignment
  // According to docs: First event must have U <= lastUpdateId AND u >= lastUpdateId
  // Reuse temp_valid_events_ buffer
  ob_state->temp_valid_events_.clear();
  size_t discarded_count = 0;
  bool gap_found = false;
  bool first_valid_found = false;
  
  for (const auto& event : ob_state->buffered_events_) {
    if (!ob_state->snapshot_received_) {
      continue;
    }

    // Extract update IDs
    int64_t first_update_id = event["U"].get<int64_t>();
    int64_t final_update_id = event["u"].get<int64_t>();

    // Discard events that are completely before the snapshot
    if (final_update_id < ob_state->last_update_id_) {
      SPDLOG_DEBUG("BinanceFuturesHandler: Discarding buffered event (too old): u={} < snapshot_id={}", 
                   final_update_id, ob_state->last_update_id_);
      discarded_count++;
      continue;
    }

    // First valid event must satisfy: U <= lastUpdateId AND u >= lastUpdateId
    if (!first_valid_found) {
      if (first_update_id <= ob_state->last_update_id_ && final_update_id >= ob_state->last_update_id_) {
        first_valid_found = true;
        ob_state->temp_valid_events_.push_back(event);
        SPDLOG_DEBUG("BinanceFuturesHandler: First valid buffered event: U={}, u={}, snapshot_id={}", 
                     first_update_id, final_update_id, ob_state->last_update_id_);
      } else {
        SPDLOG_DEBUG("BinanceFuturesHandler: Skipping buffered event (alignment check failed): U={}, u={}, snapshot_id={}", 
                     first_update_id, final_update_id, ob_state->last_update_id_);
        discarded_count++;
        continue;
      }
    } else {
      // After first valid event, just add consecutive events
      ob_state->temp_valid_events_.push_back(event);
    }
  }
  
  if (gap_found) {
    SPDLOG_DEBUG("BinanceFuturesHandler: Applied {}/{} buffered events ({} discarded as stale, stopped due to gap)", 
                ob_state->temp_valid_events_.size(), ob_state->buffered_events_.size(), discarded_count);
  } else if (discarded_count > 0) {
    SPDLOG_DEBUG("BinanceFuturesHandler: Applied {}/{} buffered events ({} discarded as stale)", 
                ob_state->temp_valid_events_.size(), ob_state->buffered_events_.size(), discarded_count);
  }
}

void BinanceFuturesHandler::ApplyBufferedEventsToOrderBook(InstrumentOrderBookState* ob_state) {
  // Apply valid events
  for (const auto& event : ob_state->temp_valid_events_) {
    int64_t first_update_id = event["U"].get<int64_t>();
    int64_t final_update_id = event["u"].get<int64_t>();

    // Apply bids
    if (event.contains("b") && event["b"].is_array()) {
      for (const auto& level : event["b"]) {
        if (level.is_array() && level.size() >= 2) {
          double price = std::stod(level[0].get<std::string>());
          double qty = std::stod(level[1].get<std::string>());
          if (qty == 0.0) {
            ob_state->local_order_book_bids_.erase(price);
          } else {
            ob_state->local_order_book_bids_[price] = qty;
          }
        }
      }
    }

    // Apply asks
    if (event.contains("a") && event["a"].is_array()) {
      for (const auto& level : event["a"]) {
        if (level.is_array() && level.size() >= 2) {
          double price = std::stod(level[0].get<std::string>());
          double qty = std::stod(level[1].get<std::string>());
          if (qty == 0.0) {
            ob_state->local_order_book_asks_.erase(price);
          } else {
            ob_state->local_order_book_asks_[price] = qty;
          }
        }
      }
    }

    ob_state->last_update_id_ = final_update_id;
  }
}

void BinanceFuturesHandler::PrepareCallbackData(InstrumentOrderBookState* ob_state) {
  // Copy order book data for callback (while still holding lock)
  // Reuse temp_bids_/temp_asks_ buffers
  ob_state->temp_bids_.clear();
  ob_state->temp_asks_.clear();
  ob_state->temp_bids_.reserve(ob_state->local_order_book_bids_.size());
  ob_state->temp_asks_.reserve(ob_state->local_order_book_asks_.size());
  
  SPDLOG_TRACE("BinanceFuturesHandler: Preparing to send initial order book to callback");
  for (const auto& [price, qty] : ob_state->local_order_book_bids_) {
    ob_state->temp_bids_.emplace_back(price, qty);
  }
  for (const auto& [price, qty] : ob_state->local_order_book_asks_) {
    ob_state->temp_asks_.emplace_back(price, qty);
  }
}

void BinanceFuturesHandler::Reconnect(SharedConnection* conn) {
  if (!conn->running_.load()) {
    return;
  }

  conn->reconnect_attempt_++;
  int backoff_seconds = std::min(60, 1 << (conn->reconnect_attempt_ - 1));  // 1, 2, 4, 8, 16, 32, 60
  conn->next_reconnect_time_ = std::chrono::steady_clock::now() + std::chrono::seconds(backoff_seconds);

  SPDLOG_DEBUG("BinanceFuturesHandler: Scheduling shared connection reconnection attempt {} in {} seconds",
              conn->reconnect_attempt_, backoff_seconds);

  std::thread([this, conn]() {
    std::this_thread::sleep_until(conn->next_reconnect_time_);
    if (conn->running_.load() && !conn->connected_.load()) {
      SPDLOG_DEBUG("BinanceFuturesHandler: Reconnecting shared connection");
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
        ConnectSharedConnection(instruments);
      }
    }
  }).detach();
}

bool BinanceFuturesHandler::ParseMessage(const std::string& message,
                                     std::vector<PriceLevel>& bids,
                                     std::vector<PriceLevel>& asks) {
  // This method is kept for interface compatibility but not actively used
  // Parsing is handled in OnRead
  try {
    auto json = nlohmann::json::parse(message);

    if (json.contains("e") && json["e"] == "depthUpdate") {
      if (json.contains("b") && json["b"].is_array()) {
        for (const auto& level : json["b"]) {
          if (level.is_array() && level.size() >= 2) {
            double price = std::stod(level[0].get<std::string>());
            double qty = std::stod(level[1].get<std::string>());
            if (qty > 0.0) {
              bids.emplace_back(price, qty);
            }
          }
        }
      }

      if (json.contains("a") && json["a"].is_array()) {
        for (const auto& level : json["a"]) {
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
  } catch (const std::exception& e) {
    SPDLOG_ERROR("BinanceFuturesHandler: Failed to parse message: {}", e.what());
  }
  return false;
}

const engine::common::Instrument* BinanceFuturesHandler::FindInstrument(const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(subscribed_mutex_);
  auto it = subscribed_instruments_.find(instrument_id);
  if (it != subscribed_instruments_.end()) {
    return it->second;
  }
  return nullptr;
}

}  // namespace herm
