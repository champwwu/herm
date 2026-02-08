#include "rest_server.hpp"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <iomanip>

namespace herm {
namespace engine {
namespace common {

RestServer::RestServer() : app_name_("herm_app") {
  start_time_ = std::chrono::steady_clock::now();
}

RestServer::~RestServer() {
  Stop();
}

bool RestServer::Start(const std::string& address, uint16_t port) {
  if (running_.exchange(true)) {
    return false;  // Already running
  }

  should_stop_ = false;
  start_time_ = std::chrono::steady_clock::now();

  server_thread_ = std::thread(&RestServer::RunServer, this, address, port);
  SPDLOG_INFO("REST server starting on {}:{}", address, port);
  return true;
}

void RestServer::Stop() {
  if (!running_.exchange(false)) {
    return;  // Not running
  }

  should_stop_ = true;
  
  // Cancel acceptor and stop io_context to interrupt blocking accept()
  {
    std::lock_guard<std::mutex> lock(ioc_mutex_);
    if (acceptor_ && acceptor_->is_open()) {
      boost::system::error_code ec;
      acceptor_->cancel(ec);
      acceptor_->close(ec);
    }
    if (ioc_) {
      ioc_->stop();
    }
  }
  
  // Clear callbacks to prevent accessing destroyed objects during shutdown
  metrics_callback_ = nullptr;
  status_callback_ = nullptr;
  reload_config_callback_ = nullptr;
  stop_callback_ = nullptr;
  
  if (server_thread_.joinable()) {
    server_thread_.join();
  }
  SPDLOG_INFO("REST server stopped");
}

void RestServer::RegisterHandler(const std::string& method, const std::string& path, HttpHandler handler) {
  handlers_[method][path] = std::move(handler);
}

std::chrono::seconds RestServer::GetUptime() const {
  auto now = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::seconds>(now - start_time_);
}

void RestServer::RunServer(const std::string& address, uint16_t port) {
  try {
    // Create shared pointers so Stop() can access them
    ioc_ = std::make_shared<net::io_context>(1);
    auto endpoint = tcp::endpoint(net::ip::make_address(address), port);
    acceptor_ = std::make_shared<tcp::acceptor>(*ioc_, endpoint);
    
    SPDLOG_INFO("REST server listening on {}:{}", address, port);

    // Start accepting connections asynchronously
    StartAccept();
    
    // Run the io_context - this will process async operations
    // It will exit when ioc_->stop() is called
    ioc_->run();
    
    SPDLOG_INFO("REST server io_context stopped");
    
    // Clean up
    {
      std::lock_guard<std::mutex> lock(ioc_mutex_);
      if (acceptor_ && acceptor_->is_open()) {
        boost::system::error_code ec;
        acceptor_->close(ec);
      }
      acceptor_.reset();
      ioc_.reset();
    }
  } catch (const std::exception& e) {
    SPDLOG_ERROR("REST server error: {}", e.what());
    running_ = false;
    {
      std::lock_guard<std::mutex> lock(ioc_mutex_);
      if (acceptor_ && acceptor_->is_open()) {
        boost::system::error_code ec;
        acceptor_->close(ec);
      }
      acceptor_.reset();
      ioc_.reset();
    }
  }
}

void RestServer::StartAccept() {
  if (should_stop_.load()) {
    return;
  }
  
  // Get references to ioc and acceptor while holding lock
  std::shared_ptr<net::io_context> ioc;
  std::shared_ptr<tcp::acceptor> acceptor;
  {
    std::lock_guard<std::mutex> lock(ioc_mutex_);
    if (!acceptor_ || !ioc_ || should_stop_.load()) {
      return;
    }
    ioc = ioc_;
    acceptor = acceptor_;
  }
  
  // Don't hold lock during async_accept to avoid blocking Stop()
  auto socket = std::make_shared<tcp::socket>(*ioc);
  
  acceptor->async_accept(*socket,
    [this, socket, ioc, acceptor](boost::system::error_code ec) {
      if (ec) {
        if (ec == boost::asio::error::operation_aborted) {
          SPDLOG_DEBUG("REST server accept cancelled");
        } else {
          SPDLOG_ERROR("REST server accept error: {}", ec.message());
        }
        return;
      }
      
      // Handle the connection in a separate thread to avoid blocking accepts
      std::thread([this, socket]() {
        try {
          beast::flat_buffer buffer;
          http::request<http::string_body> req;
          http::read(*socket, buffer, req);

          http::response<http::string_body> resp;
          HandleRequest(req, resp);

          http::write(*socket, resp);
          socket->shutdown(tcp::socket::shutdown_send);
        } catch (const std::exception& e) {
          SPDLOG_ERROR("REST server request error: {}", e.what());
        }
      }).detach();
      
      // Continue accepting new connections
      if (!should_stop_.load()) {
        StartAccept();
      }
    });
}

void RestServer::HandleRequest(const http::request<http::string_body>& req, http::response<http::string_body>& resp) {
  std::string method = std::string(req.method_string());
  std::string target = std::string(req.target());

  // Check for custom handlers first
  // Try exact match first, then prefix match for paths with parameters
  auto method_it = handlers_.find(method);
  if (method_it != handlers_.end()) {
    // First try exact match
    auto path_it = method_it->second.find(target);
    if (path_it != method_it->second.end()) {
      path_it->second(req, resp);
      return;
    }
    
    // Then try prefix match (for paths like /orderbook/{id})
    for (const auto& [registered_path, handler] : method_it->second) {
      if (target.length() >= registered_path.length() && 
          target.substr(0, registered_path.length()) == registered_path &&
          (target.length() == registered_path.length() || 
           target[registered_path.length()] == '/' || 
           target[registered_path.length()] == '?')) {
        handler(req, resp);
        return;
      }
    }
  }

  // Handle standard endpoints
  if (target == "/health") {
    HandleHealth(req, resp);
  } else if (target == "/status") {
    HandleStatus(req, resp);
  } else if (target == "/metrics") {
    HandleMetrics(req, resp);
  } else if (target == "/admin/reload-config" && method == "POST") {
    HandleReloadConfig(req, resp);
  } else if (target == "/admin/stop" && method == "POST") {
    HandleStop(req, resp);
  } else {
    HandleNotFound(req, resp);
  }
}

void RestServer::HandleHealth(const http::request<http::string_body>& req, http::response<http::string_body>& resp) {
  resp.result(http::status::ok);
  resp.set(http::field::content_type, "application/json");
  nlohmann::json health_json;
  health_json["status"] = "ok";
  resp.body() = health_json.dump();
  resp.prepare_payload();
}

void RestServer::HandleStatus(const http::request<http::string_body>& req, http::response<http::string_body>& resp) {
  resp.result(http::status::ok);
  resp.set(http::field::content_type, "application/json");

  std::string status_json;
  auto callback = status_callback_;  // Copy to avoid race condition
  if (callback) {
    try {
      status_json = callback();
    } catch (const std::exception& e) {
      SPDLOG_ERROR("Status callback error: {}", e.what());
      status_json = GetDefaultStatus();
    }
  } else {
    status_json = GetDefaultStatus();
  }

  resp.body() = status_json;
  resp.prepare_payload();
}

void RestServer::HandleMetrics(const http::request<http::string_body>& req, http::response<http::string_body>& resp) {
  resp.result(http::status::ok);
  resp.set(http::field::content_type, "application/json");

  std::string metrics_json;
  auto callback = metrics_callback_;  // Copy to avoid race condition
  if (callback) {
    try {
      metrics_json = callback();
    } catch (const std::exception& e) {
      SPDLOG_ERROR("Metrics callback error: {}", e.what());
      metrics_json = GetDefaultMetrics();
    }
  } else {
    metrics_json = GetDefaultMetrics();
  }

  resp.body() = metrics_json;
  resp.prepare_payload();
}

void RestServer::HandleReloadConfig(const http::request<http::string_body>& req, http::response<http::string_body>& resp) {
  resp.set(http::field::content_type, "application/json");

  auto callback = reload_config_callback_;  // Copy to avoid race condition
  if (callback) {
    try {
      bool success = callback();
      nlohmann::json result_json;
      result_json["status"] = success ? "ok" : "error";
      result_json["message"] = success ? "Configuration reloaded" : "Failed to reload configuration";
      resp.result(success ? http::status::ok : http::status::internal_server_error);
      resp.body() = result_json.dump();
    } catch (const std::exception& e) {
      SPDLOG_ERROR("Reload config callback error: {}", e.what());
      resp.result(http::status::internal_server_error);
      nlohmann::json error_json;
      error_json["status"] = "error";
      error_json["message"] = "Failed to reload configuration";
      resp.body() = error_json.dump();
    }
  } else {
    resp.result(http::status::not_implemented);
    nlohmann::json error_json;
    error_json["status"] = "error";
    error_json["message"] = "Reload config not implemented";
    resp.body() = error_json.dump();
  }
  resp.prepare_payload();
}

void RestServer::HandleStop(const http::request<http::string_body>& req, http::response<http::string_body>& resp) {
  resp.set(http::field::content_type, "application/json");
  resp.result(http::status::ok);
  nlohmann::json stop_json;
  stop_json["status"] = "ok";
  stop_json["message"] = "Stop command received";
  resp.body() = stop_json.dump();
  resp.prepare_payload();

  auto callback = stop_callback_;  // Copy to avoid race condition
  if (callback) {
    try {
      callback();
    } catch (const std::exception& e) {
      SPDLOG_ERROR("Stop callback error: {}", e.what());
    }
  }
}

void RestServer::HandleNotFound(const http::request<http::string_body>& req, http::response<http::string_body>& resp) {
  resp.result(http::status::not_found);
  resp.set(http::field::content_type, "application/json");
  nlohmann::json error_json;
  error_json["status"] = "error";
  error_json["message"] = "Not found";
  resp.body() = error_json.dump();
  resp.prepare_payload();
}

std::string RestServer::GetDefaultStatus() const {
  nlohmann::json status_json;
  status_json["app_name"] = app_name_;
  status_json["uptime_seconds"] = GetUptime().count();
  status_json["state"] = "running";
  return status_json.dump();
}

std::string RestServer::GetDefaultMetrics() const {
  nlohmann::json metrics_json;
  metrics_json["uptime_seconds"] = GetUptime().count();
  return metrics_json.dump();
}

}  // namespace common
}  // namespace engine
}  // namespace herm
