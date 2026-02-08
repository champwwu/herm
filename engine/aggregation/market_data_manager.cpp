#include "market_data_manager.hpp"
#include <spdlog/spdlog.h>

namespace herm {
namespace market_data {

MarketDataManager::MarketDataManager(
    const std::vector<std::string>& instrument_ids,
    const herm::engine::common::ConfigManager& config,
    herm::engine::common::EventThread* event_thread)
    : instrument_ids_(instrument_ids),
      config_(config),
      event_thread_(event_thread) {
  CreateHandlers();
}

MarketDataManager::~MarketDataManager() {
  Stop();
}

void MarketDataManager::SetUpdateCallback(MarketDataUpdateCallback callback) {
  update_callback_ = std::move(callback);
}

std::vector<ExchangeInfo> MarketDataManager::LoadExchangesFromConfig(
    const std::vector<std::string>& instrument_ids) const {
  std::vector<ExchangeInfo> exchanges;
  
  // Map instrument IDs to exchanges using instrument registry
  auto& registry = herm::engine::common::InstrumentRegistry::GetInstance();
  
  // Map to track unique exchange names/types we've seen
  std::map<std::string, std::string> exchange_name_to_type;
  
  for (const auto& instrument_id : instrument_ids) {
    const auto* instrument = registry.GetInstrument(instrument_id);
    if (!instrument) {
      SPDLOG_WARN("MarketDataManager: Instrument {} not found in registry, skipping", instrument_id);
      continue;
    }
    
    std::string exchange_name = instrument->exchange;
    std::string exchange_type = exchange_name;
    
    exchange_name_to_type[exchange_name] = exchange_type;
    
    ExchangeInfo info;
    info.name = exchange_name;
    info.type = exchange_type;
    info.instrument_id = instrument_id;
    info.url = "";  // URL not needed - handlers get config from market_data section
    
    exchanges.push_back(info);
    
    SPDLOG_DEBUG("MarketDataManager: Mapped instrument {} to exchange {} (type: {})", 
                 instrument_id, exchange_name, exchange_type);
  }
  
  if (exchanges.empty()) {
    SPDLOG_WARN("MarketDataManager: No exchanges configured from instrument IDs");
  }
  
  return exchanges;
}

void MarketDataManager::CreateHandlers() {
  auto exchanges = LoadExchangesFromConfig(instrument_ids_);
  auto& factory = MarketDataHandlerFactory::GetInstance();
  auto& registry = herm::engine::common::InstrumentRegistry::GetInstance();
  
  for (const auto& exchange : exchanges) {
    const auto* instrument = registry.GetInstrument(exchange.instrument_id);
    if (!instrument) {
      SPDLOG_WARN("MarketDataManager: Instrument {} not found in registry, skipping", 
                  exchange.instrument_id);
      continue;
    }
    
    // Determine handler type based on exchange type and instrument type
    std::string handler_type = exchange.type;
    if (handler_type == "binance") {
      if (instrument->instrument_type == herm::engine::common::InstrumentType::SPOT) {
        handler_type = "binance_spot";
      } else if (instrument->instrument_type == herm::engine::common::InstrumentType::PERP) {
        handler_type = "binance_futures";
      }
    }
    
    // Create handler if not already created for this type
    if (handlers_.find(handler_type) == handlers_.end()) {
      auto handler = factory.Create(handler_type, config_);
      if (!handler) {
        SPDLOG_WARN("MarketDataManager: Failed to create handler for type: {}", handler_type);
        continue;
      }
      
      // Set update callback - forward to our callback
      handler->SetUpdateCallback([this](
          const std::string& exchange_name,
          const herm::engine::common::Instrument& instrument,
          const std::vector<herm::PriceLevel>& bids,
          const std::vector<herm::PriceLevel>& asks) {
        if (update_callback_) {
          update_callback_(exchange_name, instrument, bids, asks);
        }
      });
      
      handlers_[handler_type] = std::move(handler);
      SPDLOG_DEBUG("MarketDataManager: Created handler for exchange type: {}", handler_type);
    }
    
    // Set CPU pinning manager on handler if available
    auto* handler = handlers_[handler_type].get();
    if (event_thread_ && event_thread_->GetCPUPinningManager()) {
      handler->SetCPUPinningManager(event_thread_->GetCPUPinningManager());
    }
    
    // Subscribe to instrument
    if (!handler->Subscribe(*instrument)) {
      SPDLOG_WARN("MarketDataManager: Failed to subscribe handler {} to instrument: {}", 
                  handler_type, exchange.instrument_id);
    } else {
      SPDLOG_DEBUG("MarketDataManager: Subscribed handler {} to instrument: {}", 
                   handler_type, exchange.instrument_id);
    }
  }
}

bool MarketDataManager::Start() {
  if (running_) {
    SPDLOG_WARN("MarketDataManager: Already running");
    return false;
  }
  
  SPDLOG_INFO("MarketDataManager: Starting {} handlers", handlers_.size());
  
  bool all_connected = true;
  for (auto& [handler_type, handler] : handlers_) {
    if (!handler->Connect()) {
      SPDLOG_ERROR("MarketDataManager: Failed to connect handler: {}", handler_type);
      all_connected = false;
    } else {
      SPDLOG_INFO("MarketDataManager: Connected handler: {}", handler_type);
    }
  }
  
  running_ = true;
  return all_connected;
}

void MarketDataManager::Stop() {
  if (!running_) {
    return;
  }
  
  SPDLOG_INFO("MarketDataManager: Stopping all handlers");
  
  for (auto& [handler_type, handler] : handlers_) {
    handler->Disconnect();
    SPDLOG_DEBUG("MarketDataManager: Disconnected handler: {}", handler_type);
  }
  
  running_ = false;
}

}  // namespace market_data
}  // namespace herm
