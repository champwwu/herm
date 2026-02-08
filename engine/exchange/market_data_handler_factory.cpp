#include "market_data_handler_factory.hpp"
#include "binance_spot_handler.hpp"
#include "binance_futures_handler.hpp"
#include "bybit_handler.hpp"
#include "okx_handler.hpp"
#include "mock_handler.hpp"
#include <spdlog/spdlog.h>

namespace herm {

MarketDataHandlerFactory& MarketDataHandlerFactory::GetInstance() {
  static MarketDataHandlerFactory instance;
  return instance;
}

void MarketDataHandlerFactory::RegisterHandler(const std::string& type, HandlerCreator creator) {
  creators_[type] = std::move(creator);
}

std::unique_ptr<MarketDataHandler> MarketDataHandlerFactory::Create(
      const std::string& type,
      const engine::common::ConfigManager& config) const {
  
  // Special handling for "binance" - check instrument type from registry
  // But since we don't have instrument_id here, we'll create both handlers
  // and let Subscribe() determine which one to use based on instrument type
  if (type == "binance") {
    // For now, create spot handler as default
    // In practice, the aggregator should create separate handlers for spot and futures
    SPDLOG_DEBUG("Creating BinanceSpotHandler (use binance_spot or binance_futures for specific types)");
    return std::make_unique<BinanceSpotHandler>(config);
  }
  
  auto it = creators_.find(type);
  if (it == creators_.end()) {
    SPDLOG_ERROR("Unknown handler type: {}", type);
    return nullptr;
  }
  
  return it->second(config);
}

bool MarketDataHandlerFactory::IsRegistered(const std::string& type) const {
  return creators_.find(type) != creators_.end();
}

// Static registration of built-in handlers
namespace {
struct HandlerRegistrar {
  HandlerRegistrar() {
    auto& factory = MarketDataHandlerFactory::GetInstance();
    
    // Register Binance handlers
    factory.RegisterHandler("binance_spot", [](const engine::common::ConfigManager& config) {
      return std::make_unique<BinanceSpotHandler>(config);
    });
    
    factory.RegisterHandler("binance_futures", [](const engine::common::ConfigManager& config) {
      return std::make_unique<BinanceFuturesHandler>(config);
    });
    
    // Register Bybit handler
    factory.RegisterHandler("bybit", [](const engine::common::ConfigManager& config) {
      return std::make_unique<BybitHandler>(config);
    });
    
    // Register OKX handler
    factory.RegisterHandler("okx", [](const engine::common::ConfigManager& config) {
      return std::make_unique<OKXHandler>(config);
    });
    
    // Register Mock handler
    factory.RegisterHandler("mock", [](const engine::common::ConfigManager& config) {
      return std::make_unique<MockHandler>(config);
    });
    
    // Register Mock handler with "mok" alias
    factory.RegisterHandler("mok", [](const engine::common::ConfigManager& config) {
      return std::make_unique<MockHandler>(config);
    });
  }
};

// Global registrar instance (initialized at program startup)
static HandlerRegistrar g_registrar;
}  // anonymous namespace

}  // namespace herm
