#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <functional>
#include <map>
#include <chrono>
#include "order_book.hpp"
#include "engine/common/thread_safe_object_pool.hpp"
#include "engine/common/rcu_ptr.hpp"
#include "market_data.grpc.pb.h"

namespace herm {

/**
 * @brief Immutable snapshot of consolidated order book data
 * 
 * Used by RCU pattern - readers access immutable snapshots,
 * writers create new snapshots and atomically publish them.
 */
struct ConsolidatedSnapshot {
  std::map<double, VenuePriceLevel, std::greater<double>> venue_bids;  // Descending
  std::map<double, VenuePriceLevel> venue_asks;  // Ascending
  std::map<std::string, std::chrono::system_clock::time_point> venue_timestamps;
  
  ConsolidatedSnapshot() = default;
  
  // Non-copyable (large structure), movable
  ConsolidatedSnapshot(const ConsolidatedSnapshot&) = delete;
  ConsolidatedSnapshot& operator=(const ConsolidatedSnapshot&) = delete;
  ConsolidatedSnapshot(ConsolidatedSnapshot&&) = default;
  ConsolidatedSnapshot& operator=(ConsolidatedSnapshot&&) = default;
};

// Aggregates order books from multiple exchanges
// Maintains a consolidated order book by merging all exchange books
// Uses RCU pattern for lock-free reads
class OrderBookAggregator {
 public:
  OrderBookAggregator(const std::string& symbol);
  ~OrderBookAggregator() = default;

  // Non-copyable, movable
  OrderBookAggregator(const OrderBookAggregator&) = delete;
  OrderBookAggregator& operator=(const OrderBookAggregator&) = delete;
  OrderBookAggregator(OrderBookAggregator&&) = default;
  OrderBookAggregator& operator=(OrderBookAggregator&&) = default;

  // Update order book from a specific exchange
  // exchange_name: e.g., "binance", "bybit", "okx"
  void UpdateExchangeBook(const std::string& exchange_name,
                          const std::vector<PriceLevel>& bids,
                          const std::vector<PriceLevel>& asks);


  // Get symbol
  const std::string& GetSymbol() const { return symbol_; }

  // Clear all exchange books
  void Clear();

  // Venue tracking methods
  // Get venue contributions at a specific price level
  std::vector<VenueContribution> GetVenueContributions(double price, bool is_bid) const;

  // Get venue timestamp (last update time for a venue)
  std::chrono::system_clock::time_point GetVenueTimestamp(const std::string& venue) const;

  // Get all venue timestamps
  std::map<std::string, std::chrono::system_clock::time_point> GetAllVenueTimestamps() const;

  // Get aggregated protobuf OrderBook (for testing and external use)
  herm::engine::common::ThreadSafeObjectPool<::herm::market_data::OrderBook>::PooledObject GetPooledAggregatedOrderBook() const;


 private:
  void ConvertToProto(::herm::market_data::OrderBook* proto, 
                      const std::shared_ptr<ConsolidatedSnapshot>& snapshot) const {
    proto->set_symbol(symbol_);
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    proto->set_timestamp_us(now);

    for (const auto& [price, venue_level] : snapshot->venue_bids) {
      auto* level = proto->add_bids();
      level->set_price(price);
      level->set_quantity(venue_level.total_quantity);
      for (const auto& [venue_name, quantity] : venue_level.venue_contributions) {
        auto* venue = level->add_venues();
        venue->set_venue_name(venue_name);
        venue->set_quantity(quantity);
      }
    }

    for (const auto& [price, venue_level] : snapshot->venue_asks) {
      auto* level = proto->add_asks();
      level->set_price(price);
      level->set_quantity(venue_level.total_quantity);
      for (const auto& [venue_name, quantity] : venue_level.venue_contributions) {
        auto* venue = level->add_venues();
        venue->set_venue_name(venue_name);
        venue->set_quantity(quantity);
      }
    }

    for (const auto& [venue_name, timestamp] : snapshot->venue_timestamps) {
      auto timestamp_us = std::chrono::duration_cast<std::chrono::microseconds>(
          timestamp.time_since_epoch()).count();
      (*proto->mutable_venue_timestamps())[venue_name] = timestamp_us;
    }
  }
  
  std::string symbol_;
  
  // Per-exchange order books (protected by write_mutex_)
  std::unordered_map<std::string, std::unique_ptr<ExchangeOrderBook>> exchange_books_;
  
  // RCU-protected consolidated snapshot (lock-free reads!)
  engine::common::RCUPtr<ConsolidatedSnapshot> consolidated_;
  
  // Mutex only protects writes (exchange_books_ modifications)
  std::mutex write_mutex_;
  
  // Pre-allocated snapshot pool for double-buffering (eliminates allocations)
  static constexpr size_t SNAPSHOT_POOL_SIZE = 3;
  std::array<std::shared_ptr<ConsolidatedSnapshot>, SNAPSHOT_POOL_SIZE> snapshot_pool_;
  std::atomic<size_t> next_snapshot_idx_{0};
  
  // Get next available snapshot from pool (cycles through pool)
  std::shared_ptr<ConsolidatedSnapshot> GetNextSnapshot();

  // Rebuild consolidated book from all exchange books into new snapshot
  void RebuildConsolidated(ConsolidatedSnapshot* snapshot) const;
};

}  // namespace herm
