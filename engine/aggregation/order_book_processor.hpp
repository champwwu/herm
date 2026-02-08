#pragma once

#include "engine/common/event_thread.hpp"
#include "engine/common/instrument_registry.hpp"
#include "engine/market_data/order_book_aggregator.hpp"
#include "market_data.grpc.pb.h"
#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace herm {
namespace market_data {

/**
 * @brief Aggregates and filters order books from multiple venues
 * 
 * Central component that:
 * - Maintains per-symbol order book aggregators
 * - Receives updates from MarketDataManager
 * - Converts C++ order books to protobuf format
 * - Filters based on request type (BBO, price bands, volume bands)
 * - Implements lock-free backpressure mechanism
 * 
 * Thread-safe for concurrent updates and queries.
 */
class OrderBookProcessor {
 public:
  // Lifecycle
  OrderBookProcessor(const std::vector<std::string>& instrument_ids,
                    engine::common::EventThread* event_thread);
  ~OrderBookProcessor();
  
  // Update processing
  /** @brief Process incoming market data update */
  void OnOrderBookUpdate(const std::string& exchange_name,
                        const engine::common::Instrument& instrument,
                        const std::vector<herm::PriceLevel>& bids,
                        const std::vector<herm::PriceLevel>& asks);
  
  // Query operations
  /** @brief Get order book by instrument ID */
  bool GetOrderBookByInstrumentId(const std::string& instrument_id, 
                                  OrderBook* output);
  
  /** @brief Get filtered order book for client request */
  void GetRequestedOrderBook(const std::string& symbol,
                       RequestType request_type,
                       const std::vector<int32_t>& basis_points,
                       const std::vector<double>& notional_volumes,
                       ::herm::market_data::OrderBook* output);
  
  /** @brief Get all tracked symbols */
  std::vector<std::string> GetSymbols() const;
  
  // Backpressure management
  /** @brief Check if symbol has pending updates */
  bool HasPendingUpdate(const std::string& symbol);
  
  /** @brief Clear pending update flag for symbol */
  void ClearPendingUpdate(const std::string& symbol);
  
  /** @brief Clear all instrument flags for symbol */
  void ClearInstrumentFlags(const std::string& symbol);

 private:
  /** @brief Extract symbol from instrument ID (e.g., "BTCUSDT.SPOT.BNC" -> "BTCUSDT") */
  std::string ExtractSymbol(const std::string& instrument_id) const;
  
  // Configuration
  std::vector<std::string> instrument_ids_;
  std::string primary_symbol_;
  engine::common::EventThread* event_thread_;
  
  // Per-symbol aggregators
  std::map<std::string, std::unique_ptr<OrderBookAggregator>> aggregators_;
  
  // Backpressure: lock-free update tracking
  std::map<std::string, std::atomic<bool>> instrument_update_flags_;
  std::map<std::string, std::atomic<bool>> symbol_update_flags_;
  std::map<std::string, std::vector<std::string>> symbol_to_instruments_;
};

}  // namespace market_data
}  // namespace herm
