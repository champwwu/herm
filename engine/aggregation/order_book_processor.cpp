#include "order_book_processor.hpp"
#include "order_book_resampler.hpp"
#include "engine/common/thread_safe_object_pool.hpp"
#include <spdlog/spdlog.h>
#include <chrono>
#include <set>
#include <algorithm>
#include <cmath>

namespace herm {
namespace market_data {

OrderBookProcessor::OrderBookProcessor(
    const std::vector<std::string>& instrument_ids,
    herm::engine::common::EventThread* event_thread)
    : instrument_ids_(instrument_ids),
      event_thread_(event_thread) {
  
  // Extract symbols from instrument IDs and create aggregators for each symbol
  std::set<std::string> symbols;
  for (const auto& instrument_id : instrument_ids) {
    std::string symbol = ExtractSymbol(instrument_id);
    symbols.insert(symbol);
  }
  
  // Create aggregators for each unique symbol
  for (const auto& symbol : symbols) {
    aggregators_[symbol] = std::make_unique<OrderBookAggregator>(symbol);
    SPDLOG_INFO("OrderBookProcessor: Created aggregator for symbol: {}", symbol);
  }
  
  // Initialize symbol flags
  for (const auto& [symbol, aggregator] : aggregators_) {
    symbol_update_flags_[symbol] = false;
  }
  
  // Initialize instrument flags and build reverse mapping
  for (const auto& instrument_id : instrument_ids) {
    instrument_update_flags_[instrument_id] = false;
    std::string symbol = ExtractSymbol(instrument_id);
    symbol_to_instruments_[symbol].push_back(instrument_id);
  }
  
  // Set primary symbol from first instrument ID
  if (!instrument_ids_.empty()) {
    primary_symbol_ = ExtractSymbol(instrument_ids_[0]);
  } else {
    primary_symbol_ = "UNKNOWN";
    SPDLOG_WARN("OrderBookProcessor: No instrument IDs provided");
  }
}

OrderBookProcessor::~OrderBookProcessor() = default;

std::string OrderBookProcessor::ExtractSymbol(const std::string& instrument_id) const {
  size_t dot_pos = instrument_id.find('.');
  return (dot_pos != std::string::npos) ? instrument_id.substr(0, dot_pos) : instrument_id;
}

void OrderBookProcessor::OnOrderBookUpdate(
    const std::string& exchange_name,
    const herm::engine::common::Instrument& instrument,
    const std::vector<herm::PriceLevel>& bids,
    const std::vector<herm::PriceLevel>& asks) {
  
  SPDLOG_TRACE("OrderBookProcessor: Received update from {} - {} - {} bids, {} asks", 
                exchange_name, instrument.instrument_id, bids.size(), asks.size());
  
  std::string symbol = ExtractSymbol(instrument.instrument_id);
  
  // Find the aggregator for this symbol
  auto agg_it = aggregators_.find(symbol);
  if (agg_it == aggregators_.end()) {
    SPDLOG_WARN("OrderBookProcessor: No aggregator found for symbol: {}", symbol);
    return;
  }
  const auto& aggregator = agg_it->second;
  
  // Update the specific aggregator for this symbol
  aggregator->UpdateExchangeBook(instrument.instrument_id, bids, asks);
  
  // Check instrument-level flag (for warning about dropped updates)
  bool was_pending = instrument_update_flags_[instrument.instrument_id].exchange(true, 
                                                                                 std::memory_order_acq_rel);
  if (was_pending) {
    SPDLOG_WARN("OrderBookProcessor: Unprocessed update dropped for instrument: {}", 
                instrument.instrument_id);
  }
  
  // Set symbol-level flag (for processing detection)
  symbol_update_flags_[symbol].store(true, std::memory_order_release);
}

bool OrderBookProcessor::GetOrderBookByInstrumentId(const std::string& instrument_id, 
                                                     OrderBook* output) {
  if (!output) {
    return false;
  }

  std::string symbol = ExtractSymbol(instrument_id);

  // Check if symbol matches the primary symbol or any of our symbols
  if (symbol != primary_symbol_) {
    bool found = false;
    for (const auto& id : instrument_ids_) {
      if (ExtractSymbol(id) == symbol) {
        found = true;
        break;
      }
    }
    if (!found) {
      return false;
    }
  }

  // Find the aggregator for this symbol
  auto agg_it = aggregators_.find(symbol);
  if (agg_it == aggregators_.end()) {
    return false;
  }

  // Convert to proto format
  auto pooled_full_book = agg_it->second->GetPooledAggregatedOrderBook();
  
  // Return false if the order book is empty (no bids and no asks)
  if (pooled_full_book->bids_size() == 0 && pooled_full_book->asks_size() == 0) {
    return false;
  }
  
  output->CopyFrom(*pooled_full_book.getPtr());
  return true;
}


void OrderBookProcessor::GetRequestedOrderBook(
    const std::string& symbol,
    RequestType request_type,
    const std::vector<int32_t>& basis_points,
    const std::vector<double>& notional_volumes,
    ::herm::market_data::OrderBook* output) {
  
  // Get full order book proto first using shared thread-safe pool
  // static auto order_book_pool = herm::engine::common::GetThreadSafeObjectPool<OrderBook>();
  // Get full order book proto first using shared thread-safe pool
  auto agg_it = aggregators_.find(symbol);
  if (agg_it == aggregators_.end()) {
    return;
  }
  auto pooled_full_book = agg_it->second->GetPooledAggregatedOrderBook();
  // Delegate to resampler
  OrderBookResampler::ResampleOrderBook(
      *pooled_full_book.getPtr(), request_type, basis_points, notional_volumes, output);
}

bool OrderBookProcessor::HasPendingUpdate(const std::string& symbol) {
  bool expected = true;
  return symbol_update_flags_[symbol].compare_exchange_strong(
      expected, false, std::memory_order_acq_rel, std::memory_order_acquire);
}

void OrderBookProcessor::ClearPendingUpdate(const std::string& symbol) {
  symbol_update_flags_[symbol].store(false, std::memory_order_release);
}

void OrderBookProcessor::ClearInstrumentFlags(const std::string& symbol) {
  auto inst_it = symbol_to_instruments_.find(symbol);
  if (inst_it != symbol_to_instruments_.end()) {
    for (const auto& inst_id : inst_it->second) {
      instrument_update_flags_[inst_id].store(false, std::memory_order_release);
    }
  }
}

std::vector<std::string> OrderBookProcessor::GetSymbols() const {
  std::vector<std::string> symbols;
  symbols.reserve(aggregators_.size());
  for (const auto& [symbol, _] : aggregators_) {
    symbols.push_back(symbol);
  }
  return symbols;
}

}  // namespace market_data
}  // namespace herm
