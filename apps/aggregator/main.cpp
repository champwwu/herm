/**
 * @file aggregator/main.cpp
 * @brief Market data aggregator service
 * 
 * Aggregates order books from multiple cryptocurrency exchanges and provides:
 * - gRPC streaming API with filtering (BBO, price bands, volume bands)
 * - REST API for querying order books by instrument ID
 * 
 * Connects to: Binance (spot/futures), OKX (spot), Bybit (perpetual)
 * 
 * Configuration: config/aggregator.json
 * - Instrument IDs to subscribe
 * - Exchange WebSocket URLs
 * - gRPC server port
 * - CPU core pinning
 * 
 * Usage:
 *   ./aggregator --config=config/aggregator.json
 * 
 * REST API:
 *   GET /orderbook?instrument_id=BTCUSDT.SPOT.BNC
 * 
 * gRPC API:
 *   StreamMarketData(symbol, request_type, basis_points, notional_volumes)
 */

#include "aggregator_service.hpp"
#include "application_kernel.hpp"
#include "engine/common/config_manager.hpp"
#include "grpc_server.hpp"
#include <boost/beast/http.hpp>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

using namespace herm::engine::common;
namespace http = boost::beast::http;

/**
 * @brief Aggregator application using ApplicationKernel
 * 
 * Lifecycle:
 * 1. OnInitialize: Parse config, create service, register REST handlers
 * 2. OnStart: Connect to exchanges, start gRPC server
 * 3. OnStop: Disconnect exchanges, shutdown gRPC
 */
class AggregatorApp : public ApplicationKernel {
 public:
  AggregatorApp() {
    SetAppName("aggregator");
  }

 protected:
  void OnInitialize() override {
    // Parse configuration
    ParseConfiguration();
    
    // Create service with parsed instrument IDs
    // Pass worker thread for event-driven processing
    service_ = std::make_shared<herm::market_data::AggregatorServiceImpl>(
        instrument_ids_, GetConfig(), &GetEventThread());
    
    // Register REST endpoint for order book by instrument ID
    GetRestServer().RegisterHandler("GET", "/orderbook", 
      [this](const auto& req, auto& resp) {
        HandleOrderBookRequest(req, resp);
      });
  }

  void OnStart() override {
    // Start the aggregator service (connects to exchanges)
    service_->Start();
    
    // Start gRPC server using standardized wrapper
    if (!grpc_server_.Start(server_address_, service_.get(), true)) {
      throw std::runtime_error("Failed to start gRPC server");
    }
  }

  void OnStop() override {
    // Stop service first (this will cancel all gRPC streams)
    if (service_) {
      service_->Stop();
    }
    
    // Shutdown gRPC server (handles cleanup internally)
    grpc_server_.Stop();
  }

 private:
  void HandleOrderBookRequest(const http::request<http::string_body>& req,
                              http::response<http::string_body>& resp) {
    // Parse instrument_id from path or query parameter
    std::string instrument_id = ExtractInstrumentId(req);
    
    if (instrument_id.empty()) {
      resp.result(http::status::bad_request);
      nlohmann::json error_json;
      error_json["error"] = "Missing instrument_id parameter";
      resp.body() = error_json.dump();
      resp.set(http::field::content_type, "application/json");
      return;
    }

    // Get order book from service
    herm::market_data::OrderBook proto_book;
    if (!service_->GetOrderBookByInstrumentId(instrument_id, &proto_book)) {
      resp.result(http::status::not_found);
      nlohmann::json error_json;
      error_json["error"] = "Order book not found for instrument: " + instrument_id;
      resp.body() = error_json.dump();
      resp.set(http::field::content_type, "application/json");
      return;
    }

    // Convert order book to JSON and send response
    nlohmann::json json_obj = ConvertOrderBookToJson(instrument_id, proto_book);
    resp.result(http::status::ok);
    resp.body() = json_obj.dump();
    resp.set(http::field::content_type, "application/json");
  }
  
  std::string ExtractInstrumentId(const http::request<http::string_body>& req) {
    std::string instrument_id;
    auto target = req.target();
    std::string target_str(target.begin(), target.end());
    
    // Check if it's /orderbook/{instrument_id} format (path parameter)
    if (target_str.length() > 11 && target_str.substr(0, 11) == "/orderbook/") {
      instrument_id = target_str.substr(11);
      // Remove query string if present
      size_t q_pos = instrument_id.find('?');
      if (q_pos != std::string::npos) {
        instrument_id = instrument_id.substr(0, q_pos);
      }
    } else {
      // Try query parameter: /orderbook?instrument_id={id}
      size_t q_pos = target_str.find('?');
      if (q_pos != std::string::npos) {
        std::string query = target_str.substr(q_pos + 1);
        size_t eq_pos = query.find('=');
        if (eq_pos != std::string::npos && query.substr(0, eq_pos) == "instrument_id") {
          instrument_id = query.substr(eq_pos + 1);
        }
      }
    }
    
    return instrument_id;
  }
  
  nlohmann::json ConvertOrderBookToJson(const std::string& instrument_id,
                                        const herm::market_data::OrderBook& proto_book) {
    nlohmann::json json_obj;
    json_obj["instrument_id"] = instrument_id;
    json_obj["symbol"] = proto_book.symbol();
    json_obj["timestamp_us"] = proto_book.timestamp_us();
    
    // Convert bids array
    json_obj["bids"] = ConvertPriceLevelsToJson(proto_book, true);
    
    // Convert asks array
    json_obj["asks"] = ConvertPriceLevelsToJson(proto_book, false);
    
    // Add venue timestamps
    if (proto_book.venue_timestamps_size() > 0) {
      nlohmann::json timestamps_obj;
      for (const auto& pair : proto_book.venue_timestamps()) {
        timestamps_obj[pair.first] = pair.second;
      }
      json_obj["venue_timestamps"] = timestamps_obj;
    }
    
    return json_obj;
  }
  
  nlohmann::json ConvertPriceLevelsToJson(const herm::market_data::OrderBook& proto_book, bool is_bids) {
    nlohmann::json levels_array = nlohmann::json::array();
    
    int size = is_bids ? proto_book.bids_size() : proto_book.asks_size();
    for (int i = 0; i < size; ++i) {
      const auto& level = is_bids ? proto_book.bids(i) : proto_book.asks(i);
      nlohmann::json level_json;
      level_json["price"] = level.price();
      level_json["quantity"] = level.quantity();
      
      // Add venue contributions if present
      if (level.venues_size() > 0) {
        nlohmann::json venues_array = nlohmann::json::array();
        for (int j = 0; j < level.venues_size(); ++j) {
          nlohmann::json venue_json;
          venue_json["venue_name"] = level.venues(j).venue_name();
          venue_json["quantity"] = level.venues(j).quantity();
          venues_array.push_back(venue_json);
        }
        level_json["venues"] = venues_array;
      }
      
      levels_array.push_back(level_json);
    }
    
    return levels_array;
  }
  
  void ParseConfiguration() {
    // Get RPC port from strategy.rpc_port
    int rpc_port = GetConfig().GetInt("strategy.rpc_port", 50051);
    server_address_ = "0.0.0.0:" + std::to_string(rpc_port);
    
    // Load instrument IDs from config
    // Format: {"symbol": {"BTCUSDT": ["BTCUSDT.SPOT.BNC", ...]}}
    instrument_ids_.clear();
    
    nlohmann::json symbol_node = GetConfig().GetNodeValue("strategy.symbol");
    
    if (!symbol_node.is_object()) {
      throw std::runtime_error("Config 'strategy.symbol' must be an object mapping symbols to instrument ID arrays");
    }
    
    for (const auto& [symbol, ids_array] : symbol_node.items()) {
      if (!ids_array.is_array()) {
        SPDLOG_WARN("Symbol '{}' value is not an array, skipping", symbol);
        continue;
      }
      
      for (const auto& id : ids_array) {
        if (id.is_string()) {
          instrument_ids_.push_back(id.get<std::string>());
        }
      }
    }
    
    if (instrument_ids_.empty()) {
      throw std::runtime_error("No instrument IDs found in config. Please configure 'strategy.symbol' field.");
    }
    
    SPDLOG_DEBUG("Loaded {} instrument IDs from config", instrument_ids_.size());
    for (const auto& id : instrument_ids_) {
      SPDLOG_TRACE("  - {}", id);
    }
  }
  
  std::shared_ptr<herm::market_data::AggregatorServiceImpl> service_;
  GrpcServer grpc_server_;
  std::vector<std::string> instrument_ids_;
  std::string server_address_;
};

int main(int argc, char** argv) {
  AggregatorApp app;
  return app.Run(argc, argv);
}
