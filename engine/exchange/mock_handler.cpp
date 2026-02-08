#include "mock_handler.hpp"
#include "engine/common/cpu_pinning.hpp"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

namespace herm {

MockHandler::MockHandler(const engine::common::ConfigManager& config)
    : MarketDataHandler("mock"),
      config_(&config) {
  Initialize(config);
}

MockHandler::~MockHandler() {
  Disconnect();
}

void MockHandler::Initialize(const engine::common::ConfigManager& config) {
  config_ = &config;
  
  // Parse mock config section from market_data.mock
  auto mock_node = config.GetNodeValue("market_data.mock");
  if (mock_node.is_object() && mock_node.contains("websocket_url") && mock_node["websocket_url"].is_string()) {
    websocket_url_ = mock_node["websocket_url"].get<std::string>();
  } else {
    // Default URL
    websocket_url_ = "ws://localhost:8084";
  }
  
  SPDLOG_DEBUG("MockHandler initialized - websocket_url: {}", websocket_url_);
}

bool MockHandler::Subscribe(const engine::common::Instrument& instrument) {
  std::lock_guard<std::mutex> lock(subscribed_mutex_);
  
  // Check if already subscribed
  if (subscribed_instruments_.find(instrument.instrument_id) != subscribed_instruments_.end()) {
    SPDLOG_WARN("MockHandler: Already subscribed to instrument: {}", instrument.instrument_id);
    return false;
  }
  
  // Add to subscribed instruments
  subscribed_instruments_[instrument.instrument_id] = &instrument;
  
  SPDLOG_DEBUG("MockHandler: Subscribed to instrument: {}", instrument.instrument_id);
  
  // Create connection for this instrument
  auto* conn = GetOrCreateConnection(instrument.instrument_id);
  if (!conn) {
    SPDLOG_ERROR("MockHandler: Failed to create connection for instrument: {}", instrument.instrument_id);
    subscribed_instruments_.erase(instrument.instrument_id);
    return false;
  }
  
  // Connect if not already connected
  if (!conn->connected_.load()) {
    if (!ConnectInstrument(instrument, conn)) {
      SPDLOG_ERROR("MockHandler: Failed to connect for instrument: {}", instrument.instrument_id);
      // Disconnect properly before removing to ensure IO thread is stopped
      DisconnectInstrument(conn);
      RemoveConnection(instrument.instrument_id);
      subscribed_instruments_.erase(instrument.instrument_id);
      return false;
    }
  }
  
  return true;
}

bool MockHandler::Unsubscribe(const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(subscribed_mutex_);
  
  auto it = subscribed_instruments_.find(instrument_id);
  if (it == subscribed_instruments_.end()) {
    SPDLOG_WARN("MockHandler: Not subscribed to instrument: {}", instrument_id);
    return false;
  }
  
  // Disconnect and remove connection
  {
    std::lock_guard<std::mutex> conn_lock(connections_mutex_);
    auto conn_it = connections_.find(instrument_id);
    if (conn_it != connections_.end()) {
      DisconnectInstrument(conn_it->second.get());
    }
    connections_.erase(instrument_id);
  }
  
  // Remove from subscribed instruments
  subscribed_instruments_.erase(it);
  
  SPDLOG_DEBUG("MockHandler: Unsubscribed from instrument: {}", instrument_id);
  return true;
}

bool MockHandler::Connect() {
  // Connect all subscribed instruments
  std::lock_guard<std::mutex> lock(subscribed_mutex_);
  
  bool all_connected = true;
  for (const auto& [instrument_id, instrument] : subscribed_instruments_) {
    auto* conn = GetOrCreateConnection(instrument_id);
    if (conn && !conn->connected_.load()) {
      if (!ConnectInstrument(*instrument, conn)) {
        all_connected = false;
      }
    }
  }
  
  return all_connected;
}

void MockHandler::Disconnect() {
  std::lock_guard<std::mutex> lock(connections_mutex_);
  
  for (auto& [instrument_id, conn] : connections_) {
    DisconnectInstrument(conn.get());
  }
  
  connections_.clear();
  
  std::lock_guard<std::mutex> subscribed_lock(subscribed_mutex_);
  subscribed_instruments_.clear();
}

bool MockHandler::IsConnected() const {
  std::lock_guard<std::mutex> lock(connections_mutex_);
  
  for (const auto& [instrument_id, conn] : connections_) {
    if (conn->connected_.load()) {
      return true;
    }
  }
  
  return false;
}

MockInstrumentConnection* MockHandler::GetOrCreateConnection(const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(connections_mutex_);
  
  auto it = connections_.find(instrument_id);
  if (it != connections_.end()) {
    return it->second.get();
  }
  
  auto conn = std::make_unique<MockInstrumentConnection>();
  conn->instrument_id = instrument_id;
  auto* conn_ptr = conn.get();
  connections_[instrument_id] = std::move(conn);
  
  return conn_ptr;
}

void MockHandler::RemoveConnection(const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(connections_mutex_);
  connections_.erase(instrument_id);
}

bool MockHandler::ConnectInstrument(const engine::common::Instrument& instrument, MockInstrumentConnection* conn) {
  if (conn->connected_.load()) {
    return true;
  }
  
  try {
    conn->ioc_ = std::make_unique<net::io_context>();
    conn->resolver_ = std::make_unique<tcp::resolver>(*conn->ioc_);
    conn->ws_ = std::make_unique<websocket::stream<beast::tcp_stream>>(*conn->ioc_);
    
    // Parse URL (simple: assume ws://host:port format)
    std::string host, port_str = "80";
    size_t protocol_end = websocket_url_.find("://");
    if (protocol_end != std::string::npos) {
      std::string rest = websocket_url_.substr(protocol_end + 3);
      size_t colon = rest.find(':');
      if (colon != std::string::npos) {
        host = rest.substr(0, colon);
        port_str = rest.substr(colon + 1);
      } else {
        host = rest;
      }
    } else {
      host = websocket_url_;
    }
    
    SPDLOG_DEBUG("MockHandler: Connecting to {}:{} for instrument {}", host, port_str, instrument.instrument_id);
    
    conn->resolver_->async_resolve(host, port_str,
        [this, conn, &instrument, host](beast::error_code ec, tcp::resolver::results_type results) {
          if (ec) {
            SPDLOG_ERROR("MockHandler resolve error for {}: {}", instrument.instrument_id, ec.message());
            return;
          }
          OnConnect(conn, ec, *results.begin());
        });
    
    conn->running_ = true;
    conn->io_thread_ = std::thread(&MockHandler::RunIO, this, conn);
    
    // Wait a bit for connection
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    return conn->connected_.load();
  } catch (const std::exception& e) {
    SPDLOG_ERROR("MockHandler connect error for {}: {}", instrument.instrument_id, e.what());
    return false;
  }
}

void MockHandler::DisconnectInstrument(MockInstrumentConnection* conn) {
  if (!conn) return;
  
  if (conn->running_.exchange(false)) {
    if (conn->ws_ && conn->ws_->is_open()) {
      conn->ws_->close(websocket::close_code::normal);
    }
    if (conn->ioc_) {
      conn->ioc_->stop();
    }
    if (conn->io_thread_.joinable()) {
      conn->io_thread_.join();
    }
    conn->connected_ = false;
    SPDLOG_DEBUG("MockHandler disconnected for instrument: {}", conn->instrument_id);
  }
}

void MockHandler::RunIO(MockInstrumentConnection* conn) {
  // Pin to CPU core if manager is set
  if (cpu_pinning_manager_) {
    try {
      cpu_pinning_manager_->PinCurrentThread("mock_io");
    } catch (const std::exception& e) {
      SPDLOG_ERROR("Failed to pin mock_io thread: {}", e.what());
      conn->running_ = false;
      if (conn->ioc_) {
        conn->ioc_->stop();
      }
      return;
    }
  }
  
  if (conn->ioc_) {
    while (conn->running_.load() && conn->ioc_) {
      conn->ioc_->run();
      if (conn->running_.load()) {
        conn->ioc_->restart();
      }
    }
  }
}

void MockHandler::OnConnect(MockInstrumentConnection* conn, beast::error_code ec, tcp::resolver::results_type::endpoint_type endpoint) {
  if (ec) {
    SPDLOG_ERROR("MockHandler connect error for {}: {}", conn->instrument_id, ec.message());
    return;
  }
  
  conn->ws_->next_layer().async_connect(endpoint,
      [this, conn](beast::error_code ec) {
        if (ec) {
          SPDLOG_ERROR("MockHandler socket connect error for {}: {}", conn->instrument_id, ec.message());
          return;
        }
        
        conn->ws_->async_handshake("", "/",
            [this, conn](beast::error_code ec) {
              OnHandshake(conn, ec);
            });
      });
}

void MockHandler::OnHandshake(MockInstrumentConnection* conn, beast::error_code ec) {
  if (ec) {
    SPDLOG_ERROR("MockHandler handshake error for {}: {}", conn->instrument_id, ec.message());
    return;
  }
  
  conn->connected_ = true;
  SPDLOG_DEBUG("MockHandler connected for instrument {}", conn->instrument_id);
  DoRead(conn);
}

void MockHandler::DoRead(MockInstrumentConnection* conn) {
  if (!conn->running_.load() || !conn->ws_ || !conn->ws_->is_open()) return;
  
  auto buffer = std::make_shared<beast::flat_buffer>();
  conn->ws_->async_read(*buffer,
      [this, conn, buffer](beast::error_code ec, std::size_t bytes_transferred) {
        OnRead(conn, ec, bytes_transferred, std::move(*buffer));
      });
}

void MockHandler::OnRead(MockInstrumentConnection* conn, beast::error_code ec, std::size_t bytes_transferred, beast::flat_buffer buffer) {
  if (ec == websocket::error::closed) {
    SPDLOG_DEBUG("MockHandler WebSocket closed for {}", conn->instrument_id);
    conn->connected_ = false;
    return;
  }
  
  if (ec) {
    // During shutdown, "End of file" errors are expected when connections are closed
    if (!conn->running_.load() && ec == boost::asio::error::eof) {
      SPDLOG_DEBUG("MockHandler connection closed during shutdown for {}", conn->instrument_id);
    } else {
      SPDLOG_ERROR("MockHandler read error for {}: {}", conn->instrument_id, ec.message());
    }
    return;
  }
  
  std::string message(static_cast<const char*>(buffer.data().data()), buffer.size());
  SPDLOG_TRACE("MockHandler: Received message ({} bytes) for {}: {}", bytes_transferred, conn->instrument_id, message);
  
  std::vector<PriceLevel> bids, asks;
  
  if (ParseMessage(message, bids, asks)) {
    SPDLOG_TRACE("MockHandler: Parsed {} bids, {} asks - notifying update", bids.size(), asks.size());
    
    // Get instrument reference for callback
    const engine::common::Instrument* instrument = FindInstrument(conn->instrument_id);
    if (instrument) {
      NotifyUpdate(*instrument, bids, asks);
    } else {
      SPDLOG_ERROR("MockHandler: Instrument {} not found for callback", conn->instrument_id);
    }
  } else {
    SPDLOG_TRACE("MockHandler: Failed to parse message");
  }
  
  DoRead(conn);
}

bool MockHandler::ParseMessage(const std::string& message,
                               std::vector<PriceLevel>& bids,
                               std::vector<PriceLevel>& asks) {
  // Parse JSON: {"bids":[[price,qty],...],"asks":[[price,qty],...]}
  bids.clear();
  asks.clear();
  
  try {
    auto json = nlohmann::json::parse(message);
    
    // Parse bids array
    if (json.contains("bids") && json["bids"].is_array()) {
      for (const auto& bid : json["bids"]) {
        if (bid.is_array() && bid.size() >= 2) {
          double price = bid[0].get<double>();
          double qty = bid[1].get<double>();
          if (qty > 0.0) {
            bids.emplace_back(price, qty);
          }
        }
      }
      SPDLOG_TRACE("MockHandler::ParseMessage: Parsed {} bids from JSON", bids.size());
    }
    
    // Parse asks array
    if (json.contains("asks") && json["asks"].is_array()) {
      for (const auto& ask : json["asks"]) {
        if (ask.is_array() && ask.size() >= 2) {
          double price = ask[0].get<double>();
          double qty = ask[1].get<double>();
          if (qty > 0.0) {
            asks.emplace_back(price, qty);
          }
        }
      }
      SPDLOG_TRACE("MockHandler::ParseMessage: Parsed {} asks from JSON", asks.size());
    }
    
    bool success = !bids.empty() || !asks.empty();
    SPDLOG_TRACE("MockHandler::ParseMessage: Result - success={}, total_bids={}, total_asks={}", 
                  success, bids.size(), asks.size());
    return success;
    
  } catch (const nlohmann::json::exception& e) {
    SPDLOG_WARN("MockHandler: JSON parse error: {}", e.what());
    return false;
  } catch (const std::exception& e) {
    SPDLOG_WARN("MockHandler: Parse error: {}", e.what());
    return false;
  }
}

const engine::common::Instrument* MockHandler::FindInstrument(const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(subscribed_mutex_);
  auto it = subscribed_instruments_.find(instrument_id);
  if (it != subscribed_instruments_.end()) {
    return it->second;
  }
  return nullptr;
}

}  // namespace herm
