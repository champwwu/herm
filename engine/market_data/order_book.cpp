#include "order_book.hpp"

#include <algorithm>
#include <stdexcept>

namespace herm {

//==============================================================================
// Constructor
//==============================================================================

ExchangeOrderBook::ExchangeOrderBook(const std::string& symbol) 
    : symbol_(symbol) {
}

//==============================================================================
// Update operations
//==============================================================================

void ExchangeOrderBook::UpdateBid(double price, double quantity) {
  std::unique_lock<std::shared_mutex> lock(mutex_);  // Exclusive lock for writes
  if (quantity > 0.0) {
    bids_[price] = quantity;
  } else {
    bids_.erase(price);
  }
}

void ExchangeOrderBook::UpdateAsk(double price, double quantity) {
  std::unique_lock<std::shared_mutex> lock(mutex_);  // Exclusive lock for writes
  if (quantity > 0.0) {
    asks_[price] = quantity;
  } else {
    asks_.erase(price);
  }
}

void ExchangeOrderBook::RemoveBid(double price) {
  std::unique_lock<std::shared_mutex> lock(mutex_);  // Exclusive lock for writes
  bids_.erase(price);
}

void ExchangeOrderBook::RemoveAsk(double price) {
  std::unique_lock<std::shared_mutex> lock(mutex_);  // Exclusive lock for writes
  asks_.erase(price);
}

void ExchangeOrderBook::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);  // Exclusive lock for writes
  bids_.clear();
  asks_.clear();
}

//==============================================================================
// Query operations
//==============================================================================

bool ExchangeOrderBook::GetBestBid(double& price, double& quantity) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
  if (bids_.empty()) {
    return false;
  }
  const auto& best = bids_.begin();
  price = best->first;
  quantity = best->second;
  return true;
}

bool ExchangeOrderBook::GetBestAsk(double& price, double& quantity) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
  if (asks_.empty()) {
    return false;
  }
  const auto& best = asks_.begin();
  price = best->first;
  quantity = best->second;
  return true;
}

std::vector<PriceLevel> ExchangeOrderBook::GetBids() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
  std::vector<PriceLevel> result;
  result.reserve(bids_.size());
  for (const auto& [price, quantity] : bids_) {
    result.emplace_back(price, quantity);
  }
  return result;
}

std::vector<PriceLevel> ExchangeOrderBook::GetAsks() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
  std::vector<PriceLevel> result;
  result.reserve(asks_.size());
  for (const auto& [price, quantity] : asks_) {
    result.emplace_back(price, quantity);
  }
  return result;
}

std::vector<PriceLevel> ExchangeOrderBook::GetBids(int depth) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
  std::vector<PriceLevel> result;
  int count = 0;
  for (const auto& [price, quantity] : bids_) {
    if (count >= depth) break;
    result.emplace_back(price, quantity);
    ++count;
  }
  return result;
}

std::vector<PriceLevel> ExchangeOrderBook::GetAsks(int depth) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
  std::vector<PriceLevel> result;
  int count = 0;
  for (const auto& [price, quantity] : asks_) {
    if (count >= depth) break;
    result.emplace_back(price, quantity);
    ++count;
  }
  return result;
}

bool ExchangeOrderBook::IsEmpty() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
  return bids_.empty() && asks_.empty();
}

//==============================================================================
// Volume calculations
//==============================================================================

double ExchangeOrderBook::GetBidVolumeUpTo(double max_price) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
  double total = 0.0;
  for (const auto& [price, quantity] : bids_) {
    if (price <= max_price) {
      total += price * quantity;
    }
  }
  return total;
}

double ExchangeOrderBook::GetAskVolumeUpTo(double min_price) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
  double total = 0.0;
  for (const auto& [price, quantity] : asks_) {
    if (price >= min_price) {
      total += price * quantity;
    }
  }
  return total;
}

bool ExchangeOrderBook::GetBidPriceForVolume(double notional_volume, double& price) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
  double cumulative = 0.0;
  for (const auto& [p, q] : bids_) {
    cumulative += p * q;
    if (cumulative >= notional_volume) {
      price = p;
      return true;
    }
  }
  return false;
}

bool ExchangeOrderBook::GetAskPriceForVolume(double notional_volume, double& price) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
  double cumulative = 0.0;
  for (const auto& [p, q] : asks_) {
    cumulative += p * q;
    if (cumulative >= notional_volume) {
      price = p;
      return true;
    }
  }
  return false;
}

}  // namespace herm
