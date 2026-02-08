#pragma once

#include "market_data_handler.hpp"
#include "engine/common/config_manager.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <map>

namespace herm {

namespace beast = boost::beast;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

// Per-instrument connection state for Mock
struct MockInstrumentConnection {
  std::string instrument_id;
  std::unique_ptr<net::io_context> ioc_;
  std::unique_ptr<tcp::resolver> resolver_;
  std::unique_ptr<websocket::stream<beast::tcp_stream>> ws_;
  std::thread io_thread_;
  std::atomic<bool> connected_{false};
  std::atomic<bool> running_{false};
};

// Mock exchange WebSocket handler
class MockHandler : public MarketDataHandler {
 public:
  MockHandler(const engine::common::ConfigManager& config);
  ~MockHandler() override;
  
  void Initialize(const engine::common::ConfigManager& config) override;
  bool Subscribe(const engine::common::Instrument& instrument) override;
  bool Unsubscribe(const std::string& instrument_id) override;
  bool Connect() override;
  void Disconnect() override;
  bool IsConnected() const override;
  
 private:
  const engine::common::ConfigManager* config_;
  
  // Configuration
  std::string websocket_url_;
  
  // Per-instrument connections
  std::map<std::string, std::unique_ptr<MockInstrumentConnection>> connections_;
  mutable std::mutex connections_mutex_;
  
  // Methods for instrument connection management
  MockInstrumentConnection* GetOrCreateConnection(const std::string& instrument_id);
  void RemoveConnection(const std::string& instrument_id);
  
  // WebSocket connection methods (per instrument)
  bool ConnectInstrument(const engine::common::Instrument& instrument, MockInstrumentConnection* conn);
  void DisconnectInstrument(MockInstrumentConnection* conn);
  
  // IO thread methods (per instrument)
  void RunIO(MockInstrumentConnection* conn);
  void OnConnect(MockInstrumentConnection* conn, beast::error_code ec, tcp::resolver::results_type::endpoint_type);
  void OnHandshake(MockInstrumentConnection* conn, beast::error_code ec);
  void DoRead(MockInstrumentConnection* conn);
  void OnRead(MockInstrumentConnection* conn, beast::error_code ec, std::size_t bytes_transferred, beast::flat_buffer buffer);
  
  // Helper to find instrument from connection
  const engine::common::Instrument* FindInstrument(const std::string& instrument_id) const;
  
  bool ParseMessage(const std::string& message,
                   std::vector<PriceLevel>& bids,
                   std::vector<PriceLevel>& asks) override;
};

}  // namespace herm
