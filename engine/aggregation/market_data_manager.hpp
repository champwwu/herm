#pragma once

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <functional>
#include "engine/exchange/market_data_handler.hpp"
#include "engine/exchange/market_data_handler_factory.hpp"
#include "engine/common/config_manager.hpp"
#include "engine/common/event_thread.hpp"
#include "engine/common/instrument_registry.hpp"

namespace herm {
namespace market_data {

// Exchange adapter info
struct ExchangeInfo {
  std::string name;
  std::string type;
  std::string url;
  std::string instrument_id;
};

// Callback type for market data updates
using MarketDataUpdateCallback = std::function<void(
    const std::string& exchange_name,
    const herm::engine::common::Instrument& instrument,
    const std::vector<herm::PriceLevel>& bids,
    const std::vector<herm::PriceLevel>& asks)>;

/**
 * @brief Manages market data connections and subscriptions to exchanges
 * 
 * Responsibilities:
 * - Load exchange configurations
 * - Create and manage market data handlers
 * - Subscribe/unsubscribe to instruments
 * - Start/stop connections
 * - Forward market data updates via callbacks
 */
class MarketDataManager {
public:
  MarketDataManager(const std::vector<std::string>& instrument_ids,
                   const herm::engine::common::ConfigManager& config,
                   herm::engine::common::EventThread* event_thread);
  
  ~MarketDataManager();
  
  // Set callback for market data updates
  void SetUpdateCallback(MarketDataUpdateCallback callback);
  
  // Start connecting to all configured exchanges
  bool Start();
  
  // Stop all connections
  void Stop();
  
  // Check if running
  bool IsRunning() const { return running_; }

private:
  const std::vector<std::string>& instrument_ids_;
  const herm::engine::common::ConfigManager& config_;
  herm::engine::common::EventThread* event_thread_;
  
  // Store handlers dynamically (one handler per exchange type)
  std::map<std::string, std::unique_ptr<herm::MarketDataHandler>> handlers_;
  
  bool running_{false};
  MarketDataUpdateCallback update_callback_;
  
  // Load exchange configuration from instrument IDs
  std::vector<ExchangeInfo> LoadExchangesFromConfig(const std::vector<std::string>& instrument_ids) const;
  
  // Create handlers for all configured exchanges
  void CreateHandlers();
};

}  // namespace market_data
}  // namespace herm
