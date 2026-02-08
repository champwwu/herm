#include "market_data_handler.hpp"
#include <spdlog/spdlog.h>
#include <mutex>

namespace herm {

MarketDataHandler::MarketDataHandler(const std::string& exchange_name)
    : exchange_name_(exchange_name) {
}

std::vector<const engine::common::Instrument*> MarketDataHandler::GetSubscribedInstruments() const {
  std::lock_guard<std::mutex> lock(subscribed_mutex_);
  std::vector<const engine::common::Instrument*> result;
  result.reserve(subscribed_instruments_.size());
  for (const auto& [id, instrument] : subscribed_instruments_) {
    result.push_back(instrument);
  }
  return result;
}

void MarketDataHandler::SetUpdateCallback(OrderBookUpdateCallback callback) {
  update_callback_ = std::move(callback);
}

void MarketDataHandler::SetCPUPinningManager(engine::common::CPUPinningManager* manager) {
  cpu_pinning_manager_ = manager;
}

void MarketDataHandler::NotifyUpdate(const engine::common::Instrument& instrument,
                                     const std::vector<PriceLevel>& bids,
                                     const std::vector<PriceLevel>& asks) {
  SPDLOG_TRACE("MarketDataHandler::NotifyUpdate: {} - {} - {} bids, {} asks", 
                exchange_name_, instrument.instrument_id, bids.size(), asks.size());
  if (update_callback_) {
    SPDLOG_TRACE("MarketDataHandler::NotifyUpdate: Calling callback for {} - {}", 
                  exchange_name_, instrument.instrument_id);
    update_callback_(exchange_name_, instrument, bids, asks);
  } else {
    SPDLOG_TRACE("MarketDataHandler::NotifyUpdate: No callback set for {} - {}", 
                  exchange_name_, instrument.instrument_id);
  }
}

}  // namespace herm
