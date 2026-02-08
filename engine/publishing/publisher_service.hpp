#pragma once

#include "engine/common/config_manager.hpp"
#include "engine/common/event_thread.hpp"
#include "market_data.grpc.pb.h"
#include "order_book_json_converter.hpp"
#include "publisher_strategy.hpp"
#include <grpcpp/grpcpp.h>
#include <map>
#include <memory>
#include <string>

namespace herm {
namespace engine {
namespace common {
class CPUPinningManager;
}  // namespace common

namespace publishing {

/**
 * @brief Manages gRPC streaming subscriptions for market data
 * 
 * Connects to aggregator service and manages multiple strategy streams.
 * Each strategy subscribes to filtered market data (BBO, price bands, volume bands).
 * Thread-safe with per-strategy mutex protection.
 */
class PublisherService {
 public:
  // Lifecycle
  PublisherService(common::ConfigManager& config,
                   common::EventThread* event_thread);
  ~PublisherService();
  
  // Non-copyable, movable
  PublisherService(const PublisherService&) = delete;
  PublisherService& operator=(const PublisherService&) = delete;
  PublisherService(PublisherService&&) = default;
  PublisherService& operator=(PublisherService&&) = default;
  
  // Control
  /** @brief Connect to server and start all strategy streams */
  bool Start(const std::string& server_target);
  
  /** @brief Stop all streams and disconnect */
  void Stop();
  
  // Data access
  /** @brief Get latest order book for strategy */
  bool GetLatestOrderBook(size_t strategy_id, 
                         herm::market_data::OrderBook& book) const;
  
  /** @brief Get latest order book as JSON */
  std::string GetLatestOrderBookJson(size_t strategy_id) const;
  
  // Query
  /** @brief Get first strategy ID (for single-strategy apps) */
  size_t GetFirstStrategyId() const;
  
  /** @brief Check if strategies configured */
  bool HasStrategies() const { return !strategies_.empty(); }
  
  // Testing/Debug
  /** @brief Clear cached order book for strategy */
  void ClearOrderBookCache(size_t strategy_id);
  
  /** @brief Clear all cached order books */
  void ClearAllOrderBookCaches();
  
  // Configuration
  /** @brief Set CPU pinning for stream threads */
  void SetCPUPinningManager(common::CPUPinningManager* manager);
  
 private:
  // Configuration loading
  /** @brief Load strategies from config file */
  bool LoadStrategiesFromConfig();
  
  /** @brief Parse single strategy from JSON */
  bool ParseStrategyConfig(const nlohmann::json& strategy_json, 
                          size_t index, 
                          StrategyConfig& config);
  
  // Stream management
  /** @brief Start gRPC stream for strategy */
  void StartStream(size_t strategy_id, const StrategyConfig& config);
  
  // Member variables
  common::ConfigManager& config_;
  common::EventThread* event_thread_;
  common::CPUPinningManager* cpu_pinning_manager_{nullptr};
  
  std::map<size_t, std::shared_ptr<StrategyData>> strategies_;
  std::map<size_t, StrategyConfig> strategy_configs_;
  std::shared_ptr<herm::market_data::MarketDataService::Stub> stub_;
};

}  // namespace publishing
}  // namespace engine
}  // namespace herm
