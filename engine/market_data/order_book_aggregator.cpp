#include "order_book_aggregator.hpp"

#include <algorithm>
#include <map>
#include <chrono>

namespace herm {

OrderBookAggregator::OrderBookAggregator(const std::string& symbol)
    : symbol_(symbol), consolidated_() {
  // Pre-allocate snapshot pool for double-buffering (eliminates allocations in hot path)
  for (size_t i = 0; i < SNAPSHOT_POOL_SIZE; ++i) {
    snapshot_pool_[i] = std::make_shared<ConsolidatedSnapshot>();
  }
  
  // Initialize with first snapshot from pool
  consolidated_.Update(snapshot_pool_[0]);
}

void OrderBookAggregator::UpdateExchangeBook(
    const std::string& exchange_name,
    const std::vector<PriceLevel>& bids,
    const std::vector<PriceLevel>& asks) {
  std::lock_guard<std::mutex> lock(write_mutex_);  // Serialize writers only
  
  // Get or create exchange book
  auto it = exchange_books_.find(exchange_name);
  if (it == exchange_books_.end()) {
    exchange_books_[exchange_name] = std::make_unique<ExchangeOrderBook>(symbol_);
    it = exchange_books_.find(exchange_name);
  }
  const auto& exchange_book = it->second;
  
  // Clear and rebuild exchange book
  exchange_book->Clear();
  for (const auto& bid : bids) {
    exchange_book->UpdateBid(bid.price, bid.quantity);
  }
  for (const auto& ask : asks) {
    it->second->UpdateAsk(ask.price, ask.quantity);
  }
  
  // Reuse snapshot from pool (NO ALLOCATION!)
  auto new_snapshot = GetNextSnapshot();
  
  // Clear maps (reuses existing memory, no deallocation!)
  new_snapshot->venue_bids.clear();
  new_snapshot->venue_asks.clear();
  new_snapshot->venue_timestamps.clear();
  
  // Update venue timestamp
  new_snapshot->venue_timestamps[exchange_name] = std::chrono::system_clock::now();
  
  // Copy timestamps from current snapshot for other venues
  auto current_snapshot = consolidated_.Read();
  for (const auto& [venue, timestamp] : current_snapshot->venue_timestamps) {
    if (venue != exchange_name) {
      new_snapshot->venue_timestamps[venue] = timestamp;
    }
  }
  
  // Rebuild consolidated book into snapshot (reuses map memory)
  RebuildConsolidated(new_snapshot.get());
  
  // Atomically publish new snapshot (lock-free for readers!)
  consolidated_.Update(new_snapshot);
}


void OrderBookAggregator::Clear() {
  std::lock_guard<std::mutex> lock(write_mutex_);
  exchange_books_.clear();
  
  // Reuse snapshot from pool
  auto new_snapshot = GetNextSnapshot();
  new_snapshot->venue_bids.clear();
  new_snapshot->venue_asks.clear();
  new_snapshot->venue_timestamps.clear();
  consolidated_.Update(new_snapshot);
}

std::shared_ptr<ConsolidatedSnapshot> OrderBookAggregator::GetNextSnapshot() {
  // Cycle through pre-allocated snapshots (lock-free)
  // SNAPSHOT_POOL_SIZE of 3 ensures at least one snapshot is always free:
  // - One being read by multiple readers
  // - One being written by writer
  // - One free for next update
  size_t idx = next_snapshot_idx_.fetch_add(1, std::memory_order_relaxed) % SNAPSHOT_POOL_SIZE;
  return snapshot_pool_[idx];
}

void OrderBookAggregator::RebuildConsolidated(ConsolidatedSnapshot* snapshot) const {
  // Note: venue_bids and venue_asks already cleared by caller
  // Reusing existing map capacity avoids allocations
  
  // Iterate through all exchange order books and build venue contribution maps
  for (const auto& [exchange_name, book] : exchange_books_) {
    book->ForEachBid([&](double price, double quantity) {
      auto it = snapshot->venue_bids.find(price);
      if (it == snapshot->venue_bids.end()) {
        snapshot->venue_bids[price] = VenuePriceLevel(price);
      }
      snapshot->venue_bids[price].AddVenue(exchange_name, quantity);
    });

    book->ForEachAsk([&](double price, double quantity) {
      auto it = snapshot->venue_asks.find(price);
      if (it == snapshot->venue_asks.end()) {
        snapshot->venue_asks[price] = VenuePriceLevel(price);
      }
      snapshot->venue_asks[price].AddVenue(exchange_name, quantity);
    });
  }
}

std::vector<VenueContribution> OrderBookAggregator::GetVenueContributions(double price, bool is_bid) const {
  // Lock-free read of consolidated snapshot
  auto snapshot = consolidated_.Read();
  
  if (is_bid) {
    auto it = snapshot->venue_bids.find(price);
    if (it != snapshot->venue_bids.end()) {
      return it->second.GetContributions();
    }
  } else {
    auto it = snapshot->venue_asks.find(price);
    if (it != snapshot->venue_asks.end()) {
      return it->second.GetContributions();
    }
  }
  
  return {};  // Empty if price not found
}

std::chrono::system_clock::time_point OrderBookAggregator::GetVenueTimestamp(const std::string& venue) const {
  // Lock-free read of consolidated snapshot
  auto snapshot = consolidated_.Read();
  auto it = snapshot->venue_timestamps.find(venue);
  if (it != snapshot->venue_timestamps.end()) {
    return it->second;
  }
  return std::chrono::system_clock::time_point::min();  // Not found
}

std::map<std::string, std::chrono::system_clock::time_point> OrderBookAggregator::GetAllVenueTimestamps() const {
  // Lock-free read of consolidated snapshot
  auto snapshot = consolidated_.Read();
  return snapshot->venue_timestamps;
}

herm::engine::common::ThreadSafeObjectPool<::herm::market_data::OrderBook>::PooledObject 
OrderBookAggregator::GetPooledAggregatedOrderBook() const {
  static auto order_book_pool = herm::engine::common::GetThreadSafeObjectPool<::herm::market_data::OrderBook>();
  auto pooled_book = order_book_pool->GetPooledObject();
  
  // Lock-free read of consolidated snapshot
  auto snapshot = consolidated_.Read();
  
  // Convert snapshot to protobuf (no lock needed, snapshot is immutable)
  ConvertToProto(pooled_book.getPtr(), snapshot);
  return std::move(pooled_book);
}

}  // namespace herm
