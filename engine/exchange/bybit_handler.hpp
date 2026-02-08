#pragma once

#include "market_data_handler.hpp"
#include "rate_limiter.hpp"
#include "bybit_http_client.hpp"
#include "shared_connection.hpp"
#include "engine/common/config_manager.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <vector>
#include <map>
#include <set>
#include <functional>
#include <chrono>
#include <nlohmann/json.hpp>

namespace herm {

namespace beast = boost::beast;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = boost::asio::ip::tcp;

class BybitHandler : public MarketDataHandler {
 public:
  BybitHandler(const engine::common::ConfigManager& config);
  ~BybitHandler() override;
  
  void Initialize(const engine::common::ConfigManager& config) override;
  bool Subscribe(const engine::common::Instrument& instrument) override;
  bool Unsubscribe(const std::string& instrument_id) override;
  bool Connect() override;
  void Disconnect() override;
  bool IsConnected() const override;

 private:
  const engine::common::ConfigManager* config_;
  
  // HTTP client and rate limiter (shared across all instruments)
  std::unique_ptr<BybitHttpClient> http_client_;
  std::unique_ptr<RateLimiter> rate_limiter_;
  
  // Configuration
  std::string websocket_url_spot_;
  std::string websocket_url_perp_;
  std::string rest_url_;
  int rate_limit_rps_ = 20;
  
  // Shared connections: one for SPOT, one for PERP
  std::unique_ptr<SharedConnection> spot_connection_;
  std::unique_ptr<SharedConnection> perp_connection_;
  mutable std::mutex connection_mutex_;
  
  // Per-instrument order book states
  std::map<std::string, InstrumentOrderBookState> order_books_;
  mutable std::mutex order_books_mutex_;
  
  // Symbol to instrument ID mapping (exchange symbol -> instrument_id)
  std::map<std::string, std::string> symbol_to_instrument_id_;
  mutable std::mutex symbol_mapping_mutex_;
  
  // Methods for shared connection management
  SharedConnection* GetOrCreateSharedConnection(engine::common::InstrumentType type);
  bool EnsureConnected(engine::common::InstrumentType type);
  void DisconnectSharedConnection(engine::common::InstrumentType type);
  
  // WebSocket connection methods
  bool ConnectSharedConnection(engine::common::InstrumentType type, const std::vector<const engine::common::Instrument*>& instruments);
  bool ConnectSharedConnectionWithRetry(engine::common::InstrumentType type, const std::vector<const engine::common::Instrument*>& instruments, int max_retries = 3);
  
  // IO thread methods
  void RunIO(SharedConnection* conn);
  void OnHandshake(SharedConnection* conn, engine::common::InstrumentType type, beast::error_code ec);
  void DoRead(SharedConnection* conn);
  void OnRead(SharedConnection* conn, beast::error_code ec, std::size_t bytes_transferred, beast::flat_buffer buffer);
  void HandlePing(SharedConnection* conn);
  
  // Subscription management
  bool SubscribeInstrument(const engine::common::Instrument& instrument);
  void UnsubscribeInstrument(const std::string& instrument_id);
  
  // Snapshot management (per order book state)
  void GetSnapshot(const engine::common::Instrument& instrument);
  void ApplySnapshot(const std::string& instrument_id, const BybitHttpClient::DepthSnapshot& snapshot);
  void ApplyWebSocketSnapshot(InstrumentOrderBookState* order_book_state, const nlohmann::json& event);
  void ApplyUpdate(InstrumentOrderBookState* order_book_state, const nlohmann::json& event);
  bool ValidateUpdateId(InstrumentOrderBookState* order_book_state, int64_t update_id, int64_t seq);
  void ReconnectAll(engine::common::InstrumentType type);
  
  // Symbol conversion and routing
  std::string GetBybitSymbol(const engine::common::Instrument& instrument) const;
  InstrumentOrderBookState* FindOrderBookBySymbol(const std::string& exchange_symbol);
  std::string GetInstrumentIdFromSymbol(const std::string& exchange_symbol) const;
  
  // Build WebSocket URL based on instrument type
  std::string GetWebSocketUrl(const engine::common::Instrument& instrument) const;
  
  // Build subscription message for multiple instruments
  std::string BuildSubscriptionMessage(const std::vector<const engine::common::Instrument*>& instruments) const;
  std::string BuildUnsubscriptionMessage(const std::string& exchange_symbol) const;
  
  // Message parsing
  bool ParseMessage(const std::string& message,
                   std::vector<PriceLevel>& bids,
                   std::vector<PriceLevel>& asks) override;
  
  // Helper to find instrument
  const engine::common::Instrument* FindInstrument(const std::string& instrument_id) const;
};

}  // namespace herm
