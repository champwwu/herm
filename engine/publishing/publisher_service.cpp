#include "publisher_service.hpp"
#include "engine/common/event_thread.hpp"
#include "engine/common/cpu_pinning.hpp"
#include <spdlog/spdlog.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/grpcpp.h>
#include <iomanip>
#include <iostream>
#include <chrono>
#include <ctime>
#include <sstream>

namespace herm {
namespace engine {
namespace publishing {

PublisherService::PublisherService(herm::engine::common::ConfigManager& config,
                                   herm::engine::common::EventThread* event_thread)
    : config_(config), event_thread_(event_thread) {
}

PublisherService::~PublisherService() {
  Stop();
}

void PublisherService::SetCPUPinningManager(herm::engine::common::CPUPinningManager* manager) {
  cpu_pinning_manager_ = manager;
}

bool PublisherService::Start(const std::string& server_target) {
  // Create gRPC channel
  auto channel = grpc::CreateChannel(server_target, grpc::InsecureChannelCredentials());
  if (!channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(10))) {
    SPDLOG_ERROR("Failed to connect to {}", server_target);
    return false;
  }
  
  auto stub = herm::market_data::MarketDataService::NewStub(channel);
  stub_ = std::shared_ptr<herm::market_data::MarketDataService::Stub>(stub.release());
  
  // Load strategies from config
  if (!LoadStrategiesFromConfig()) {
    return false;
  }
  
  // Start streams for all strategies
  for (const auto& [id, config] : strategy_configs_) {
    StartStream(id, config);
    SPDLOG_INFO("Registered strategy {}: request={}, symbol={}", 
                 id, config.request_type, config.symbol_root);
  }
  
  return true;
}

void PublisherService::Stop() {
  // Cancel all gRPC contexts first to interrupt blocking reads
  for (auto& [id, data] : strategies_) {
    data->active = false;
    if (data->grpc_context) {
      data->grpc_context->TryCancel();
    }
  }
  
  // Wait for threads to finish
  for (auto& [id, data] : strategies_) {
    if (data->stream_thread.joinable()) {
      data->stream_thread.join();
    }
  }
  
  strategies_.clear();
  strategy_configs_.clear();
}

bool PublisherService::GetLatestOrderBook(size_t strategy_id, herm::market_data::OrderBook& book) const {
  auto it = strategies_.find(strategy_id);
  if (it == strategies_.end()) {
    return false;
  }
  
  std::lock_guard<std::mutex> lock(it->second->mutex);
  book = it->second->latest_book;
  return true;
}

std::string PublisherService::GetLatestOrderBookJson(size_t strategy_id) const {
  herm::market_data::OrderBook book;
  if (!GetLatestOrderBook(strategy_id, book)) {
    return "";
  }
  
  return OrderBookJsonConverter::ToJson(book);
}

size_t PublisherService::GetFirstStrategyId() const {
  if (strategies_.empty()) {
    return SIZE_MAX;
  }
  return strategies_.begin()->first;
}

void PublisherService::ClearOrderBookCache(size_t strategy_id) {
  auto it = strategies_.find(strategy_id);
  if (it != strategies_.end()) {
    std::lock_guard<std::mutex> lock(it->second->mutex);
    it->second->latest_book.Clear();
    SPDLOG_INFO("Cleared order book cache for strategy {}", strategy_id);
  }
}

void PublisherService::ClearAllOrderBookCaches() {
  for (auto& [id, data] : strategies_) {
    std::lock_guard<std::mutex> lock(data->mutex);
    data->latest_book.Clear();
  }
  SPDLOG_INFO("Cleared order book cache for all strategies");
}

bool PublisherService::LoadStrategiesFromConfig() {
  nlohmann::json subscribe_node = config_.GetNodeValue("strategy.subscribe");
  if (!subscribe_node.is_array()) {
    SPDLOG_ERROR("No subscribe array found in config");
    return false;
  }
  
  for (size_t i = 0; i < subscribe_node.size(); ++i) {
    const auto& strategy_config = subscribe_node[i];
    
    StrategyConfig config;
    if (!ParseStrategyConfig(strategy_config, i, config)) {
      continue;
    }
    
    // Create strategy data
    auto strategy_data = std::make_shared<StrategyData>();
    strategies_[i] = strategy_data;
    strategy_configs_[i] = config;
  }
  
  return !strategies_.empty();
}

bool PublisherService::ParseStrategyConfig(const nlohmann::json& strategy_json, size_t index, StrategyConfig& config) {
  if (!strategy_json.contains("request") || !strategy_json["request"].is_string()) {
    SPDLOG_WARN("Strategy {} missing 'request' field, skipping", index);
    return false;
  }
  config.request_type = strategy_json["request"].get<std::string>();
  
  if (!strategy_json.contains("symbol_root") || !strategy_json["symbol_root"].is_string()) {
    SPDLOG_WARN("Strategy {} missing 'symbol_root' field, skipping", index);
    return false;
  }
  config.symbol_root = strategy_json["symbol_root"].get<std::string>();
  
  // Parse request-specific parameters
  if (config.request_type == "PriceBand") {
    if (strategy_json.contains("basis_points") && strategy_json["basis_points"].is_array()) {
      for (const auto& bp : strategy_json["basis_points"]) {
        if (bp.is_number_integer()) {
          config.basis_points.push_back(bp.get<int32_t>());
        }
      }
    }
    if (config.basis_points.empty()) {
      SPDLOG_WARN("Strategy {} (PriceBand) missing basis_points, using defaults", index);
      config.basis_points = {50, 100, 200, 500, 1000};
    }
  } else if (config.request_type == "VolumeBand") {
    if (strategy_json.contains("notional_volumes") && strategy_json["notional_volumes"].is_array()) {
      for (const auto& vol : strategy_json["notional_volumes"]) {
        if (vol.is_number()) {
          config.notional_volumes.push_back(vol.get<double>());
        }
      }
    }
    if (config.notional_volumes.empty()) {
      SPDLOG_WARN("Strategy {} (VolumeBand) missing notional_volumes, using defaults", index);
      config.notional_volumes = {1000000.0, 5000000.0, 10000000.0, 25000000.0, 50000000.0};
    }
  }
  
  return true;
}

// Helper function to format timestamp to readable format
static std::string FormatTimestamp(int64_t timestamp_us) {
  auto time_point = std::chrono::system_clock::time_point(
      std::chrono::microseconds(timestamp_us));
  auto time_t = std::chrono::system_clock::to_time_t(time_point);
  
  std::tm tm_buf;
  localtime_r(&time_t, &tm_buf);
  
  std::ostringstream oss;
  oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
  
  // Add microseconds
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(
      time_point.time_since_epoch()) % 1000000;
  oss << "." << std::setfill('0') << std::setw(6) << us.count();
  
  return oss.str();
}

// Helper function to format venue contributions
static std::string FormatVenues(const google::protobuf::RepeatedPtrField<herm::market_data::VenueContribution>& venues) {
  if (venues.empty()) {
    return "";
  }
  
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(2);
  bool first = true;
  for (const auto& venue : venues) {
    if (!first) {
      oss << ", ";
    }
    oss << venue.venue_name() << ": " << venue.quantity();
    first = false;
  }
  return oss.str();
}

// Helper function to print order book in the requested format
static void PrintOrderBook(const herm::market_data::OrderBook& book, 
                          const std::string& symbol) {
  // Print header with symbol and readable timestamp
  std::string readable_time = FormatTimestamp(book.timestamp_us());
  std::cout << "\nSymbol: " << symbol 
            << ", Timestamp: " << readable_time 
            << " (" << book.timestamp_us() << ")" << std::endl;
  std::cout << std::string(100, '-') << std::endl;
  
  // Print asks first (highest to lowest price, right-aligned)
  int ask_idx = book.asks_size();
  for (int i = book.asks_size() - 1; i >= 0; --i) {
    const auto& level = book.asks(i);
    std::ostringstream price_oss;
    price_oss << std::fixed << std::setprecision(2) << level.quantity() << " @ " << level.price();
    
    std::string venue_str = FormatVenues(level.venues());
    
    std::cout << std::left << std::setw(8) << ("ask_" + std::to_string(ask_idx--))
              << std::right << std::setw(52) << price_oss.str();
    
    if (!venue_str.empty()) {
      std::cout << "        | " << venue_str;
    }
    std::cout << std::endl;
  }
  
  // Print bids (highest to lowest price, left-aligned)
  int bid_idx = 1;
  for (int i = 0; i < book.bids_size(); ++i) {
    const auto& level = book.bids(i);
    std::ostringstream price_oss;
    price_oss << std::fixed << std::setprecision(2) << level.quantity() << " @ " << level.price();
    
    std::string venue_str = FormatVenues(level.venues());
    
    std::cout << std::left << std::setw(8) << ("bid_" + std::to_string(bid_idx++))
              << std::left << std::setw(52) << price_oss.str();
    
    if (!venue_str.empty()) {
      std::cout << "        | " << venue_str;
    }
    std::cout << std::endl;
  }
  
  if (book.bids_size() == 0 && book.asks_size() == 0) {
    std::cout << "(empty order book)" << std::endl;
  }
}

void PublisherService::StartStream(size_t strategy_id, const StrategyConfig& config) {
  auto& data = strategies_[strategy_id];
  
  SPDLOG_INFO("Starting gRPC stream for strategy {}: symbol={}, request_type={}", 
              strategy_id, config.symbol_root, config.request_type);
  
  // Create and store gRPC context so we can cancel it
  data->grpc_context = std::make_shared<grpc::ClientContext>();
  
  // Capture shared_ptr to strategy data for use in lambda
  auto strategy_data = data;
  
  data->stream_thread = std::thread([this, strategy_id, config, strategy_data]() {
    SPDLOG_INFO("gRPC stream thread started for strategy {}", strategy_id);
    // Pin to CPU core if manager is set
    if (cpu_pinning_manager_) {
      try {
        std::string thread_name = "grpc_stream_" + std::to_string(strategy_id);
        cpu_pinning_manager_->PinCurrentThread(thread_name);
      } catch (const std::exception& e) {
        SPDLOG_ERROR("Failed to pin gRPC stream thread for strategy {}: {}", strategy_id, e.what());
        // Continue without CPU pinning rather than failing the stream
      }
    }
    
    herm::market_data::StreamMarketDataRequest request;
    request.set_symbol(config.symbol_root);
    
    // Set request type
    if (config.request_type == "BBO") {
      request.set_request_type(herm::market_data::RequestType::BBO);
    } else if (config.request_type == "PriceBand") {
      request.set_request_type(herm::market_data::RequestType::PRICE_BAND);
      for (int32_t bp : config.basis_points) {
        request.add_basis_points(bp);
      }
    } else if (config.request_type == "VolumeBand") {
      request.set_request_type(herm::market_data::RequestType::VOLUME_BAND);
      for (double vol : config.notional_volumes) {
        request.add_notional_volumes(vol);
      }
    } else {
      request.set_request_type(herm::market_data::RequestType::NONE);
    }
    
    // Get the stored context
    if (!strategy_data->grpc_context) {
      SPDLOG_ERROR("Strategy {} context not found", strategy_id);
      return;
    }
    auto context = strategy_data->grpc_context;
    
    SPDLOG_INFO("Calling StreamMarketData for strategy {}, symbol={}", strategy_id, config.symbol_root);
    auto reader = stub_->StreamMarketData(context.get(), request);
    
    herm::market_data::OrderBook book;
    
    SPDLOG_INFO("Entering read loop for strategy {}", strategy_id);
    // Check active flag in the loop to allow early exit
    while (strategy_data->active.load(std::memory_order_acquire) && reader->Read(&book)) {
      SPDLOG_DEBUG("Received order book for strategy {}", strategy_id);
      // Post data processing to worker thread
      if (event_thread_) {
        event_thread_->Post([this, strategy_id, book]() {
          auto it = strategies_.find(strategy_id);
          if (it != strategies_.end()) {
            std::lock_guard<std::mutex> lock(it->second->mutex);
            it->second->latest_book = book;
          }
        });
      } else {
        // Fallback if no worker thread
        auto it = strategies_.find(strategy_id);
        if (it != strategies_.end()) {
          std::lock_guard<std::mutex> lock(it->second->mutex);
          it->second->latest_book = book;
        }
      }
      
      // Print order book in the requested format
      PrintOrderBook(book, config.symbol_root);
    }
    
    auto status = reader->Finish();
    if (!status.ok() && status.error_code() != grpc::StatusCode::CANCELLED) {
      SPDLOG_ERROR("Stream error for strategy {}: {}", strategy_id, status.error_message());
    }
    
    // Mark as inactive
    strategy_data->active = false;
  });
}

}  // namespace publishing
}  // namespace engine
}  // namespace herm
