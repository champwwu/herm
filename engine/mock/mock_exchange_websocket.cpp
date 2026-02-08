#include "mock_exchange_websocket.hpp"

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <iomanip>

namespace herm {

MockExchangeWebSocketSession::MockExchangeWebSocketSession(tcp::socket socket, MockExchange* exchange)
    : ws_(std::move(socket)), exchange_(exchange) {
}

void MockExchangeWebSocketSession::Run() {
  ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::server));
  ws_.set_option(websocket::stream_base::decorator(
      [](websocket::response_type& res) {
        res.set(http::field::server, "MockExchange");
      }));
  
  ws_.async_accept(
      beast::bind_front_handler(&MockExchangeWebSocketSession::OnAccept, shared_from_this()));
}

void MockExchangeWebSocketSession::OnAccept(beast::error_code ec) {
  if (ec) {
    SPDLOG_ERROR("WebSocket accept error: {}", ec.message());
    return;
  }
  
  SPDLOG_INFO("MockExchange: WebSocket client connected, sending initial order book");
  
  // Send initial order book
  SendOrderBookUpdate();
  SPDLOG_INFO("MockExchange: Initial order book sent, will continue sending updates every 100ms");
  
  // Start reading
  DoRead();
}

void MockExchangeWebSocketSession::DoRead() {
  ws_.async_read(buffer_,
      beast::bind_front_handler(&MockExchangeWebSocketSession::OnRead, shared_from_this()));
}

void MockExchangeWebSocketSession::OnRead(beast::error_code ec, std::size_t bytes_transferred) {
  if (ec == websocket::error::closed) {
    SPDLOG_INFO("WebSocket client disconnected");
    return;
  }
  
  if (ec) {
    SPDLOG_ERROR("WebSocket read error: {}", ec.message());
    return;
  }
  
  // Echo back or handle message
  // For now, just clear buffer and continue
  buffer_.consume(bytes_transferred);
  
  // Continue reading
  DoRead();
}

void MockExchangeWebSocketSession::SendOrderBookUpdate() {
  std::string message = FormatOrderBookAsJson();
  
  SPDLOG_INFO("MockExchange: Sending order book update ({} bytes)", message.size());
  SPDLOG_TRACE("MockExchange: Order book update content: {}", message);
  
  ws_.async_write(net::buffer(message),
      beast::bind_front_handler(&MockExchangeWebSocketSession::OnWrite, shared_from_this()));
}

void MockExchangeWebSocketSession::OnWrite(beast::error_code ec, std::size_t bytes_transferred) {
  if (ec) {
    SPDLOG_ERROR("WebSocket write error: {}", ec.message());
    return;
  }
  
  SPDLOG_INFO("MockExchange: Successfully wrote {} bytes to client", bytes_transferred);
  
  // Schedule next update
  auto self = shared_from_this();
  std::thread([self]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (self->ws_.is_open()) {
      SPDLOG_INFO("MockExchange: Scheduling next order book update");
      self->SendOrderBookUpdate();
    } else {
      SPDLOG_INFO("MockExchange: WebSocket closed, not sending update");
    }
  }).detach();
}

std::string MockExchangeWebSocketSession::FormatOrderBookAsJson() const {
  auto bids = exchange_->GetBids();
  auto asks = exchange_->GetAsks();

  SPDLOG_TRACE("MockExchange: Formatting order book - {} bids, {} asks", bids.size(), asks.size());

  nlohmann::json j;
  j["bids"] = nlohmann::json::array();
  for (const auto& bid : bids) {
    j["bids"].push_back({bid.price, bid.quantity});
  }
  j["asks"] = nlohmann::json::array();
  for (const auto& ask : asks) {
    j["asks"].push_back({ask.price, ask.quantity});
  }

  return j.dump();
}

MockExchangeWebSocketServer::MockExchangeWebSocketServer(
    const std::string& exchange_name,
    const std::string& symbol,
    const std::string& address,
    uint16_t port,
    double base_price)
    : exchange_name_(exchange_name),
      symbol_(symbol),
      address_(address),
      port_(port),
      base_price_(base_price),
      exchange_(std::make_unique<MockExchange>(exchange_name, symbol, base_price, 0.001, std::chrono::milliseconds(100))),
      acceptor_(ioc_) {
}

MockExchangeWebSocketServer::~MockExchangeWebSocketServer() {
  Stop();
}

void MockExchangeWebSocketServer::Start() {
  bool was_running = running_.load();
  bool set_running = running_.exchange(true);
  
  if (!set_running) {  // If we successfully set it to true (it was false before)
    SPDLOG_INFO("MockExchangeWebSocketServer::Start() - Starting exchange");
    exchange_->Start();
    
    beast::error_code ec;
    tcp::endpoint endpoint(net::ip::make_address(address_), port_);
    
    SPDLOG_INFO("MockExchangeWebSocketServer: Starting server on {}:{}", address_, port_);
    
    acceptor_.open(endpoint.protocol(), ec);
    if (ec) {
      SPDLOG_ERROR("Failed to open acceptor: {}", ec.message());
      running_.exchange(false);
      return;
    }
    
    acceptor_.set_option(net::socket_base::reuse_address(true), ec);
    if (ec) {
      SPDLOG_WARN("Failed to set reuse_address option: {}", ec.message());
      // Continue anyway
    }
    
    acceptor_.bind(endpoint, ec);
    if (ec) {
      SPDLOG_ERROR("Failed to bind to {}:{}: {}", address_, port_, ec.message());
      running_.exchange(false);
      return;
    }
    
    acceptor_.listen(net::socket_base::max_listen_connections, ec);
    if (ec) {
      SPDLOG_ERROR("Failed to listen on {}:{}: {}", address_, port_, ec.message());
      running_.exchange(false);
      return;
    }
    
    SPDLOG_INFO("Mock exchange WebSocket server listening on {}:{}", address_, port_);
    SPDLOG_INFO("MockExchangeWebSocketServer: Server started, waiting for connections");
    
    // Start the server thread - it will run the io_context and accept connections
    server_thread_ = std::thread([this]() {
      SPDLOG_INFO("MockExchangeWebSocketServer: Server thread started, starting AcceptLoop");
      AcceptLoop();  // Start accepting connections (schedules async_accept)
      SPDLOG_INFO("MockExchangeWebSocketServer: Starting io_context.run()");
      ioc_.run();    // Run the io_context to process async operations
      SPDLOG_INFO("MockExchangeWebSocketServer: io_context finished");
    });
    SPDLOG_INFO("MockExchangeWebSocketServer::Start() - Server thread started successfully");
  } else {
    SPDLOG_WARN("MockExchangeWebSocketServer::Start() called but already running (was_running={})", was_running);
  }
  SPDLOG_INFO("MockExchangeWebSocketServer::Start() - Exiting");
}

void MockExchangeWebSocketServer::Stop() {
  if (running_.exchange(false)) {
    exchange_->Stop();
    ioc_.stop();
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
    SPDLOG_INFO("Mock exchange WebSocket server stopped");
  }
}

void MockExchangeWebSocketServer::AcceptLoop() {
  if (!running_.load()) {
    SPDLOG_INFO("MockExchangeWebSocketServer: AcceptLoop stopped (running=false)");
    return;
  }
  
  SPDLOG_INFO("MockExchangeWebSocketServer: Waiting for new connection...");
  auto socket = std::make_shared<tcp::socket>(ioc_);
  acceptor_.async_accept(*socket,
      [this, socket](beast::error_code ec) {
        if (ec) {
          SPDLOG_ERROR("MockExchangeWebSocketServer: Accept error: {}", ec.message());
        } else {
          SPDLOG_INFO("MockExchangeWebSocketServer: New connection accepted");
        }
        OnAccept(ec, std::move(*socket));
        if (running_.load()) {
          AcceptLoop();
        }
      });
}

void MockExchangeWebSocketServer::OnAccept(beast::error_code ec, tcp::socket socket) {
  if (ec) {
    SPDLOG_ERROR("Accept error: {}", ec.message());
    return;
  }
  
  std::make_shared<MockExchangeWebSocketSession>(std::move(socket), exchange_.get())->Run();
}

bool MockExchangeWebSocketServer::LoadDeterminedOrderBookFromFile(const std::string& file_path) {
  if (exchange_) {
    return exchange_->LoadDeterminedOrderBookFromFile(file_path);
  }
  return false;
}

}  // namespace herm
