#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include "engine/market_data/order_book.hpp"
#include "engine/common/instrument_registry.hpp"

namespace herm {
namespace engine {
namespace common {
class ConfigManager;
class CPUPinningManager;
}  // namespace common
}  // namespace engine

// Callback for order book updates - includes complete Instrument definition
using OrderBookUpdateCallback = std::function<void(
    const std::string& exchange_name,
    const engine::common::Instrument& instrument,  // Complete instrument definition
    const std::vector<PriceLevel>& bids,
    const std::vector<PriceLevel>& asks)>;

// Abstract market data handler interface
class MarketDataHandler {
 public:
  MarketDataHandler(const std::string& exchange_name);
  virtual ~MarketDataHandler() = default;
  
  // Non-copyable, movable
  MarketDataHandler(const MarketDataHandler&) = delete;
  MarketDataHandler& operator=(const MarketDataHandler&) = delete;
  MarketDataHandler(MarketDataHandler&&) = default;
  MarketDataHandler& operator=(MarketDataHandler&&) = default;
  
  // Initialize handler with configuration
  virtual void Initialize(const engine::common::ConfigManager& config) = 0;
  
  // Subscribe to an instrument
  virtual bool Subscribe(const engine::common::Instrument& instrument) = 0;
  
  // Unsubscribe from an instrument
  virtual bool Unsubscribe(const std::string& instrument_id) = 0;
  
  // Get list of subscribed instruments
  std::vector<const engine::common::Instrument*> GetSubscribedInstruments() const;
  
  // Connect to exchange WebSocket (URL comes from config)
  virtual bool Connect() = 0;
  
  // Disconnect
  virtual void Disconnect() = 0;
  
  // Check if connected
  virtual bool IsConnected() const = 0;
  
  // Set callback for order book updates
  void SetUpdateCallback(OrderBookUpdateCallback callback);
  
  // Get exchange name
  const std::string& GetExchangeName() const { return exchange_name_; }
  
  // Set CPU pinning manager for IO threads
  void SetCPUPinningManager(engine::common::CPUPinningManager* manager);
  
 protected:
  std::string exchange_name_;
  std::unordered_map<std::string, const engine::common::Instrument*> subscribed_instruments_;
  OrderBookUpdateCallback update_callback_;
  mutable std::mutex subscribed_mutex_;  // Protect subscribed_instruments_ map
  engine::common::CPUPinningManager* cpu_pinning_manager_{nullptr};
  
  // Notify callback of update
  void NotifyUpdate(const engine::common::Instrument& instrument,
                    const std::vector<PriceLevel>& bids,
                    const std::vector<PriceLevel>& asks);
  
  // Parse message from exchange (implemented by subclasses)
  virtual bool ParseMessage(const std::string& message,
                           std::vector<PriceLevel>& bids,
                           std::vector<PriceLevel>& asks) = 0;
};

}  // namespace herm
