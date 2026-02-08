/**
 * @file publisher/main.cpp
 * @brief Market data publisher client
 * 
 * Subscribes to filtered market data from aggregator service and provides:
 * - REST API for latest order book (JSON format)
 * - Multiple concurrent strategy subscriptions
 * - CPU-pinned gRPC stream threads for low latency
 * 
 * Configuration: config/publisher.json
 * - Aggregator server address
 * - Strategy configurations (request type, basis points, volumes)
 * - Symbols to subscribe
 * 
 * Usage:
 *   ./publisher --config=config/publisher.json
 * 
 * REST API:
 *   GET /orderbook - Returns latest order book for first strategy
 */

#include "application_kernel.hpp"
#include "publishing/publisher_service.hpp"
#include <boost/beast/http.hpp>
#include <nlohmann/json.hpp>

using namespace herm::engine::common;
using namespace herm::engine::publishing;
namespace http = boost::beast::http;

/**
 * @brief Publisher application using ApplicationKernel
 * 
 * Subscribes to aggregator and caches latest order books locally.
 */
class PublisherApp : public ApplicationKernel {
 public:
  PublisherApp() {
    SetAppName("publisher");
  }

 protected:
  void OnInitialize() override {
    // Publisher service will be initialized in OnStart
  }

  void OnStart() override {
    std::string target = GetConfig().GetString("strategy.aggregator_server", "localhost:50051");
    
    // Create publisher service
    publisher_service_ = std::make_unique<PublisherService>(GetConfig(), &GetEventThread());
    
    // Set CPU pinning manager if available
    if (GetCPUPinningManager()) {
      publisher_service_->SetCPUPinningManager(GetCPUPinningManager());
    }
    
    // Start publisher service (loads strategies and starts streams)
    if (!publisher_service_->Start(target)) {
      SPDLOG_ERROR("Failed to start publisher service");
      return;
    }
    
    // Register REST endpoint for latest order book
    if (publisher_service_->HasStrategies()) {
      RegisterRestEndpoint();
    }
  }

  void OnStop() override {
    if (publisher_service_) {
      publisher_service_->Stop();
    }
  }

 private:
  std::unique_ptr<PublisherService> publisher_service_;
  
  void RegisterRestEndpoint() {
    // GET /latestOrderBook - Get latest order book
    GetRestServer().RegisterHandler("GET", "/latestOrderBook", [this](const auto&, auto& resp) {
      if (!publisher_service_ || !publisher_service_->HasStrategies()) {
        resp.result(http::status::service_unavailable);
        nlohmann::json error_json;
        error_json["error"] = "No strategy configured";
        resp.body() = error_json.dump();
        resp.set(http::field::content_type, "application/json");
        return;
      }
      
      size_t strategy_id = publisher_service_->GetFirstStrategyId();
      if (strategy_id == SIZE_MAX) {
        resp.result(http::status::not_found);
        resp.set(http::field::content_type, "application/json");
        nlohmann::json error_json;
        error_json["error"] = "No strategy available";
        resp.body() = error_json.dump();
        return;
      }
      
      // Get JSON from publisher service
      std::string json_str = publisher_service_->GetLatestOrderBookJson(strategy_id);
      if (json_str.empty()) {
        resp.result(http::status::service_unavailable);
        resp.set(http::field::content_type, "application/json");
        nlohmann::json error_json;
        error_json["error"] = "Order book not available";
        resp.body() = error_json.dump();
        return;
      }
      
      resp.result(http::status::ok);
      resp.set(http::field::content_type, "application/json");
      resp.body() = json_str;
    });
    
    // POST /reset - Clear order book cache (for testing)
    GetRestServer().RegisterHandler("POST", "/reset", [this](const auto&, auto& resp) {
      if (!publisher_service_) {
        resp.result(http::status::service_unavailable);
        nlohmann::json error_json;
        error_json["status"] = "error";
        error_json["message"] = "Publisher service not available";
        resp.body() = error_json.dump();
        resp.set(http::field::content_type, "application/json");
        return;
      }
      
      // Clear all order book caches
      publisher_service_->ClearAllOrderBookCaches();
      
      resp.result(http::status::ok);
      resp.set(http::field::content_type, "application/json");
      nlohmann::json success_json;
      success_json["status"] = "ok";
      success_json["message"] = "Order book cache cleared";
      resp.body() = success_json.dump();
      
      SPDLOG_INFO("Order book cache reset via REST API");
    });
  }
};

int main(int argc, char** argv) {
  PublisherApp app;
  return app.Run(argc, argv);
}
