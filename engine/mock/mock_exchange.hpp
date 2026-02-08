#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include "engine/market_data/order_book.hpp"

namespace herm {

// Mock exchange that generates realistic order book updates
// Can be used as a WebSocket server or in-memory generator
class MockExchange {
 public:
  MockExchange(const std::string& exchange_name,
               const std::string& symbol,
               double base_price = 50000.0,
               double price_volatility = 0.001,  // 0.1% volatility
               std::chrono::milliseconds update_interval = std::chrono::milliseconds(100));
  
  ~MockExchange();
  
  // Non-copyable, movable
  MockExchange(const MockExchange&) = delete;
  MockExchange& operator=(const MockExchange&) = delete;
  MockExchange(MockExchange&&) = default;
  MockExchange& operator=(MockExchange&&) = default;
  
  // Start generating updates
  void Start();
  
  // Stop generating updates
  void Stop();
  
  // Get current order book snapshot (returns const reference)
  const ExchangeOrderBook& GetOrderBook() const;
  
  // Get current bids
  std::vector<PriceLevel> GetBids() const;
  
  // Get current asks
  std::vector<PriceLevel> GetAsks() const;
  
  // Get exchange name
  const std::string& GetExchangeName() const { return exchange_name_; }
  
  // Get symbol
  const std::string& GetSymbol() const { return symbol_; }
  
  // Set update interval
  void SetUpdateInterval(std::chrono::milliseconds interval);
  
  // Set price volatility
  void SetPriceVolatility(double volatility);

  // Load determined order book from JSON data
  // JSON structure: {"bids": [{"price": X, "quantity": Y}, ...], "asks": [{"price": X, "quantity": Y}, ...]}
  bool LoadDeterminedOrderBook(const std::string& json_data);

  // Load determined order book from file
  bool LoadDeterminedOrderBookFromFile(const std::string& file_path);

 private:
  std::string exchange_name_;
  std::string symbol_;
  double base_price_;
  double price_volatility_;
  std::chrono::milliseconds update_interval_;
  
  std::unique_ptr<ExchangeOrderBook> order_book_;
  
  std::atomic<bool> running_{false};
  std::thread update_thread_;
  
  // Random number generators (mutable for const methods)
  mutable std::mt19937 rng_;
  mutable std::normal_distribution<double> price_dist_;
  mutable std::uniform_real_distribution<double> quantity_dist_;
  
  // Current mid price (drifts from base_price)
  std::atomic<double> current_mid_price_;
  
  // Flag to indicate if using determined (fixture) data
  std::atomic<bool> use_determined_data_{false};
  
  // Update loop
  void UpdateLoop();
  
  // Generate a new order book update
  void GenerateUpdate();
  
  // Generate price level
  double GeneratePrice(bool is_bid) const;
  
  // Generate quantity
  double GenerateQuantity() const;
};

}  // namespace herm
