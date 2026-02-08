#pragma once

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <map>
#include <mutex>

namespace herm {
namespace engine {
namespace common {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

// HTTP request handler type
using HttpHandler = std::function<void(
    const http::request<http::string_body>& req,
    http::response<http::string_body>& resp)>;

// REST server for admin commands and state queries
class RestServer {
 public:
  RestServer();
  ~RestServer();

  // Non-copyable, movable
  RestServer(const RestServer&) = delete;
  RestServer& operator=(const RestServer&) = delete;
  RestServer(RestServer&&) = default;
  RestServer& operator=(RestServer&&) = default;

  // Start server on specified address and port
  bool Start(const std::string& address, uint16_t port);

  // Stop server
  void Stop();

  // Check if server is running
  bool IsRunning() const { return running_.load(); }

  // Register custom endpoint handler
  void RegisterHandler(const std::string& method, const std::string& path, HttpHandler handler);

  // Get uptime in seconds
  std::chrono::seconds GetUptime() const;

  // Set application name for status endpoint
  void SetAppName(const std::string& name) { app_name_ = name; }

  // Set metrics callback (called for /metrics endpoint)
  void SetMetricsCallback(std::function<std::string()> callback) { metrics_callback_ = callback; }

  // Set status callback (called for /status endpoint)
  void SetStatusCallback(std::function<std::string()> callback) { status_callback_ = callback; }

  // Set reload config callback (called for /admin/reload-config)
  void SetReloadConfigCallback(std::function<bool()> callback) { reload_config_callback_ = callback; }

  // Set stop callback (called for /admin/stop)
  void SetStopCallback(std::function<void()> callback) { stop_callback_ = callback; }

 private:
  std::string app_name_;
  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};
  std::thread server_thread_;
  std::chrono::steady_clock::time_point start_time_;
  
  // IO context and acceptor for interruptible shutdown
  std::shared_ptr<net::io_context> ioc_;
  std::shared_ptr<tcp::acceptor> acceptor_;
  std::mutex ioc_mutex_;

  // Custom handlers: method -> path -> handler
  std::map<std::string, std::map<std::string, HttpHandler>> handlers_;

  // Callbacks for standard endpoints
  std::function<std::string()> metrics_callback_;
  std::function<std::string()> status_callback_;
  std::function<bool()> reload_config_callback_;
  std::function<void()> stop_callback_;

  void RunServer(const std::string& address, uint16_t port);
  void StartAccept();
  void HandleRequest(const http::request<http::string_body>& req, http::response<http::string_body>& resp);
  void HandleHealth(const http::request<http::string_body>& req, http::response<http::string_body>& resp);
  void HandleStatus(const http::request<http::string_body>& req, http::response<http::string_body>& resp);
  void HandleMetrics(const http::request<http::string_body>& req, http::response<http::string_body>& resp);
  void HandleReloadConfig(const http::request<http::string_body>& req, http::response<http::string_body>& resp);
  void HandleStop(const http::request<http::string_body>& req, http::response<http::string_body>& resp);
  void HandleNotFound(const http::request<http::string_body>& req, http::response<http::string_body>& resp);

  std::string GetDefaultStatus() const;
  std::string GetDefaultMetrics() const;
};

}  // namespace common
}  // namespace engine
}  // namespace herm
