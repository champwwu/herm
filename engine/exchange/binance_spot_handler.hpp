#pragma once

#include "market_data_handler.hpp"
#include "rate_limiter.hpp"
#include "binance_http_client.hpp"
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

class BinanceSpotHandler : public MarketDataHandler {
 public:
  BinanceSpotHandler(const engine::common::ConfigManager& config);
  ~BinanceSpotHandler() override;

  void Initialize(const engine::common::ConfigManager& config) override;
  bool Subscribe(const engine::common::Instrument& instrument) override;
  bool Unsubscribe(const std::string& instrument_id) override;
  bool Connect() override;
  void Disconnect() override;
  bool IsConnected() const override;

 private:
  const engine::common::ConfigManager* config_;
  
  // HTTP client and rate limiter (shared across all instruments)
  std::unique_ptr<BinanceHttpClient> http_client_;
  std::unique_ptr<RateLimiter> rate_limiter_;
  
  // Configuration
  std::string websocket_url_template_;
  std::string rest_url_;
  int rate_limit_rps_ = 20;
  
  // Shared connection for all instruments
  std::unique_ptr<SharedConnection> shared_connection_;
  mutable std::mutex connection_mutex_;
  
  // Per-instrument order book states
  std::map<std::string, InstrumentOrderBookState> order_books_;
  mutable std::mutex order_books_mutex_;
  
  // Stream to instrument ID mapping (stream name -> instrument_id)
  std::map<std::string, std::string> stream_to_instrument_id_;
  mutable std::mutex stream_mapping_mutex_;
  
  // Methods for shared connection management
  SharedConnection* GetOrCreateSharedConnection();
  bool EnsureConnected();
  void DisconnectSharedConnection();
  
  // WebSocket connection methods
  bool ConnectSharedConnection(const std::vector<const engine::common::Instrument*>& instruments);
  bool ConnectSharedConnectionWithRetry(const std::vector<const engine::common::Instrument*>& instruments, int max_retries = 3);
  
  // IO thread methods
  void RunIO(SharedConnection* conn);
  void OnHandshake(SharedConnection* conn, beast::error_code ec);
  void DoRead(SharedConnection* conn);
  void OnRead(SharedConnection* conn, beast::error_code ec, std::size_t bytes_transferred, beast::flat_buffer buffer);
  void HandlePing(SharedConnection* conn, const std::string& payload);
  
  // Subscription management
  bool SubscribeInstrument(const engine::common::Instrument& instrument);
  void UnsubscribeInstrument(const std::string& instrument_id);
  
  // Snapshot management (per order book state)
  void GetSnapshot(const engine::common::Instrument& instrument);
  void ApplySnapshot(const std::string& instrument_id, const BinanceHttpClient::DepthSnapshot& snapshot);
  void FilterBufferedEvents(InstrumentOrderBookState* order_book_state);
  void ApplyBufferedEventsToOrderBook(InstrumentOrderBookState* order_book_state);
  void PrepareCallbackData(InstrumentOrderBookState* order_book_state);
  void ApplyUpdate(InstrumentOrderBookState* order_book_state, const nlohmann::json& event);
  bool ValidateUpdateId(InstrumentOrderBookState* order_book_state, int64_t first_id, int64_t final_id);
  void Reconnect(SharedConnection* conn);
  
  // Symbol conversion and routing
  std::string GetBinanceSymbol(const engine::common::Instrument& instrument) const;
  std::string GetStreamName(const engine::common::Instrument& instrument) const;
  InstrumentOrderBookState* FindOrderBookByStream(const std::string& stream);
  std::string GetInstrumentIdFromStream(const std::string& stream) const;
  
  // Build WebSocket URL for combined stream
  std::string BuildCombinedStreamUrl(const std::vector<const engine::common::Instrument*>& instruments) const;
  
  // Message parsing
  bool ParseMessage(const std::string& message,
                   std::vector<PriceLevel>& bids,
                   std::vector<PriceLevel>& asks) override;
  
  // Helper to find instrument
  const engine::common::Instrument* FindInstrument(const std::string& instrument_id) const;
};

}  // namespace herm
