#pragma once

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include "mock_exchange.hpp"

namespace herm {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

// WebSocket session for a single client
class MockExchangeWebSocketSession : public std::enable_shared_from_this<MockExchangeWebSocketSession> {
 public:
  MockExchangeWebSocketSession(tcp::socket socket, MockExchange* exchange);
  
  void Run();
  
 private:
  websocket::stream<beast::tcp_stream> ws_;
  MockExchange* exchange_;
  beast::flat_buffer buffer_;
  
  void OnAccept(beast::error_code ec);
  void DoRead();
  void OnRead(beast::error_code ec, std::size_t bytes_transferred);
  void DoWrite();
  void OnWrite(beast::error_code ec, std::size_t bytes_transferred);
  void SendOrderBookUpdate();
  std::string FormatOrderBookAsJson() const;
};

// WebSocket server for mock exchange
class MockExchangeWebSocketServer {
 public:
  MockExchangeWebSocketServer(const std::string& exchange_name,
                              const std::string& symbol,
                              const std::string& address,
                              uint16_t port,
                              double base_price = 50000.0);
  
  ~MockExchangeWebSocketServer();
  
  // Non-copyable, movable
  MockExchangeWebSocketServer(const MockExchangeWebSocketServer&) = delete;
  MockExchangeWebSocketServer& operator=(const MockExchangeWebSocketServer&) = delete;
  MockExchangeWebSocketServer(MockExchangeWebSocketServer&&) = default;
  MockExchangeWebSocketServer& operator=(MockExchangeWebSocketServer&&) = default;
  
  void Start();
  void Stop();
  
  // Load determined order book from file
  bool LoadDeterminedOrderBookFromFile(const std::string& file_path);
  
 private:
  std::string exchange_name_;
  std::string symbol_;
  std::string address_;
  uint16_t port_;
  double base_price_;
  
  std::unique_ptr<MockExchange> exchange_;
  net::io_context ioc_;
  tcp::acceptor acceptor_;
  std::thread server_thread_;
  std::atomic<bool> running_{false};
  
  void AcceptLoop();
  void OnAccept(beast::error_code ec, tcp::socket socket);
};

}  // namespace herm
