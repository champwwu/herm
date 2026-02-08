#pragma once

#include "market_data_handler.hpp"
#include "engine/common/config_manager.hpp"
#include <memory>
#include <string>
#include <functional>
#include <unordered_map>

namespace herm {

// Factory for creating market data handlers
class MarketDataHandlerFactory {
 public:
  // Handler creator function type (no instrument_id needed)
  using HandlerCreator = std::function<std::unique_ptr<MarketDataHandler>(
      const engine::common::ConfigManager& config)>;

  // Get singleton instance
  static MarketDataHandlerFactory& GetInstance();

  // Register a handler type
  void RegisterHandler(const std::string& type, HandlerCreator creator);

  // Create a handler by type
  std::unique_ptr<MarketDataHandler> Create(
      const std::string& type,
      const engine::common::ConfigManager& config) const;

  // Check if a type is registered
  bool IsRegistered(const std::string& type) const;

 private:
  MarketDataHandlerFactory() = default;
  ~MarketDataHandlerFactory() = default;
  MarketDataHandlerFactory(const MarketDataHandlerFactory&) = delete;
  MarketDataHandlerFactory& operator=(const MarketDataHandlerFactory&) = delete;

  std::unordered_map<std::string, HandlerCreator> creators_;
};

}  // namespace herm
