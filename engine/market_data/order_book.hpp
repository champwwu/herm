#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

namespace herm {

/**
 * @brief Single price level in order book
 */
struct PriceLevel {
  double price;
  double quantity;

  PriceLevel() : price(0.0), quantity(0.0) {}
  PriceLevel(double p, double q) : price(p), quantity(q) {}

  bool operator==(const PriceLevel& other) const {
    return price == other.price && quantity == other.quantity;
  }
};

/**
 * @brief Venue contribution to a price level
 */
struct VenueContribution {
  std::string venue_name;
  double quantity;
  
  VenueContribution(const std::string& name, double qty) 
    : venue_name(name), quantity(qty) {}
};

/**
 * @brief Price level with multi-venue tracking
 * 
 * Tracks contributions from multiple venues at a single price.
 * Used for consolidated order books.
 */
struct VenuePriceLevel {
  double price;
  double total_quantity;
  std::map<std::string, double> venue_contributions;
  
  VenuePriceLevel() : price(0.0), total_quantity(0.0) {}
  explicit VenuePriceLevel(double p) : price(p), total_quantity(0.0) {}
  
  /** @brief Add or update venue contribution */
  void AddVenue(const std::string& venue, double qty) {
    auto it = venue_contributions.find(venue);
    if (it != venue_contributions.end()) {
      total_quantity -= it->second;
    }
    venue_contributions[venue] = qty;
    total_quantity += qty;
  }
  
  /** @brief Remove venue contribution */
  void RemoveVenue(const std::string& venue) {
    auto it = venue_contributions.find(venue);
    if (it != venue_contributions.end()) {
      total_quantity -= it->second;
      venue_contributions.erase(it);
    }
  }
  
  /** @brief Get all venue contributions as vector */
  std::vector<VenueContribution> GetContributions() const {
    std::vector<VenueContribution> result;
    result.reserve(venue_contributions.size());
    for (const auto& [venue, qty] : venue_contributions) {
      result.emplace_back(venue, qty);
    }
    return result;
  }
};

/**
 * @brief Order book for a single exchange and symbol
 * 
 * Thread-safe sorted order book using std::map.
 * Bids sorted descending (best bid first), asks sorted ascending (best ask first).
 */
class ExchangeOrderBook {
 public:
  // Constructor
  explicit ExchangeOrderBook(const std::string& symbol);
  ~ExchangeOrderBook() = default;

  // Non-copyable, movable
  ExchangeOrderBook(const ExchangeOrderBook&) = delete;
  ExchangeOrderBook& operator=(const ExchangeOrderBook&) = delete;
  ExchangeOrderBook(ExchangeOrderBook&&) = default;
  ExchangeOrderBook& operator=(ExchangeOrderBook&&) = default;

  // Accessors
  /** @brief Get symbol name */
  const std::string& GetSymbol() const { return symbol_; }

  // Update operations
  /** @brief Update or add bid level */
  void UpdateBid(double price, double quantity);
  
  /** @brief Update or add ask level */
  void UpdateAsk(double price, double quantity);
  
  /** @brief Remove bid level */
  void RemoveBid(double price);
  
  /** @brief Remove ask level */
  void RemoveAsk(double price);
  
  /** @brief Clear all price levels */
  void Clear();

  // Query operations
  /** @brief Get best bid (highest price), returns false if empty */
  bool GetBestBid(double& price, double& quantity) const;
  
  /** @brief Get best ask (lowest price), returns false if empty */
  bool GetBestAsk(double& price, double& quantity) const;
  
  /** @brief Get all bids sorted descending */
  std::vector<PriceLevel> GetBids() const;
  
  /** @brief Get all asks sorted ascending */
  std::vector<PriceLevel> GetAsks() const;
  
  /** @brief Get top N bids */
  std::vector<PriceLevel> GetBids(int depth) const;
  
  /** @brief Get top N asks */
  std::vector<PriceLevel> GetAsks(int depth) const;
  
  /** @brief Check if order book is empty */
  bool IsEmpty() const;

  // Iterator operations
  /** @brief Apply callback to each bid level */
  void ForEachBid(const std::function<void(double, double)>& callback) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
    for (const auto& [price, quantity] : bids_) {
      callback(price, quantity);
    }
  }

  /** @brief Apply callback to each ask level */
  void ForEachAsk(const std::function<void(double, double)>& callback) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);  // Shared lock for reads
    for (const auto& [price, quantity] : asks_) {
      callback(price, quantity);
    }
  }

  // Volume calculations
  /** @brief Calculate total bid volume up to max price */
  double GetBidVolumeUpTo(double max_price) const;
  
  /** @brief Calculate total ask volume up to min price */
  double GetAskVolumeUpTo(double min_price) const;
  
  /** @brief Find price level for target bid volume */
  bool GetBidPriceForVolume(double notional_volume, double& price) const;
  
  /** @brief Find price level for target ask volume */
  bool GetAskPriceForVolume(double notional_volume, double& price) const;

 private:
  std::string symbol_;
  std::map<double, double, std::greater<double>> bids_;  // Descending
  std::map<double, double> asks_;  // Ascending
  mutable std::shared_mutex mutex_;  // Reader-writer lock for concurrent reads
};

}  // namespace herm
