#include "mock_exchange.hpp"
#include "engine/common/config_manager.hpp"
#include <nlohmann/json.hpp>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cmath>
#include <random>
#include <spdlog/spdlog.h>

namespace herm {

MockExchange::MockExchange(const std::string& exchange_name,
                           const std::string& symbol,
                           double base_price,
                           double price_volatility,
                           std::chrono::milliseconds update_interval)
    : exchange_name_(exchange_name),
      symbol_(symbol),
      base_price_(base_price),
      price_volatility_(price_volatility),
      update_interval_(update_interval),
      order_book_(std::make_unique<ExchangeOrderBook>(symbol)),
      rng_(std::random_device{}()),
      price_dist_(0.0, price_volatility),
      quantity_dist_(0.1, 10.0),
      current_mid_price_(base_price) {
  // Initialize with some initial levels
  GenerateUpdate();
}

MockExchange::~MockExchange() {
  Stop();
}

void MockExchange::Start() {
  if (running_.exchange(true)) {
    update_thread_ = std::thread(&MockExchange::UpdateLoop, this);
    SPDLOG_INFO("Mock exchange {} started", exchange_name_);
  }
}

void MockExchange::Stop() {
  if (running_.exchange(false)) {
    if (update_thread_.joinable()) {
      update_thread_.join();
    }
    SPDLOG_INFO("Mock exchange {} stopped", exchange_name_);
  }
}

const ExchangeOrderBook& MockExchange::GetOrderBook() const {
  return *order_book_;
}

std::vector<PriceLevel> MockExchange::GetBids() const {
  return order_book_->GetBids();
}

std::vector<PriceLevel> MockExchange::GetAsks() const {
  return order_book_->GetAsks();
}

void MockExchange::SetUpdateInterval(std::chrono::milliseconds interval) {
  update_interval_ = interval;
}

void MockExchange::SetPriceVolatility(double volatility) {
  price_volatility_ = volatility;
}

void MockExchange::UpdateLoop() {
  while (running_.load()) {
    // Only generate random updates if not using determined data
    // If using determined data, the order book is static and doesn't need regeneration
    if (!use_determined_data_.load()) {
      GenerateUpdate();
    }
    std::this_thread::sleep_for(update_interval_);
  }
}

void MockExchange::GenerateUpdate() {
  // Update mid price with random walk
  double price_change = price_dist_(rng_) * current_mid_price_.load();
  double new_mid = current_mid_price_.load() + price_change;
  current_mid_price_.store(new_mid);
  
  // Generate 5-10 levels on each side
  int num_levels = 5 + (rng_() % 6);
  
  // Clear and regenerate
  order_book_->Clear();
  
  // Generate bids (below mid price)
  for (int i = 0; i < num_levels; ++i) {
    double price = GeneratePrice(true);
    double quantity = GenerateQuantity();
    order_book_->UpdateBid(price, quantity);
  }
  
  // Generate asks (above mid price)
  for (int i = 0; i < num_levels; ++i) {
    double price = GeneratePrice(false);
    double quantity = GenerateQuantity();
    order_book_->UpdateAsk(price, quantity);
  }
}

double MockExchange::GeneratePrice(bool is_bid) const {
  double mid = current_mid_price_.load();
  double spread = mid * 0.001;  // 0.1% spread
  
  if (is_bid) {
    // Bid: below mid, with some randomness
    double offset = spread * (0.5 + (rng_() % 100) / 200.0);  // 0.5x to 1.0x spread
    return mid - offset + price_dist_(rng_) * mid * 0.0001;
  } else {
    // Ask: above mid, with some randomness
    double offset = spread * (0.5 + (rng_() % 100) / 200.0);  // 0.5x to 1.0x spread
    return mid + offset + price_dist_(rng_) * mid * 0.0001;
  }
}

double MockExchange::GenerateQuantity() const {
  return quantity_dist_(rng_);
}

bool MockExchange::LoadDeterminedOrderBook(const std::string& json_data) {
  try {
    nlohmann::json node = nlohmann::json::parse(json_data);
    
    order_book_->Clear();
    
    if (node.contains("bids") && node["bids"].is_array()) {
      for (const auto& bid : node["bids"]) {
        if (bid.contains("price") && bid.contains("quantity")) {
          double price = bid["price"].get<double>();
          double qty = bid["quantity"].get<double>();
          order_book_->UpdateBid(price, qty);
        }
      }
    }
    
    if (node.contains("asks") && node["asks"].is_array()) {
      for (const auto& ask : node["asks"]) {
        if (ask.contains("price") && ask.contains("quantity")) {
          double price = ask["price"].get<double>();
          double qty = ask["quantity"].get<double>();
          order_book_->UpdateAsk(price, qty);
        }
      }
    }
    
    // Calculate mid price from loaded data
    auto bids = order_book_->GetBids();
    auto asks = order_book_->GetAsks();
    if (!bids.empty() && !asks.empty()) {
      double best_bid = bids[0].price;
      double best_ask = asks[0].price;
      current_mid_price_.store((best_bid + best_ask) / 2.0);
    }
    
    use_determined_data_.store(true);
    SPDLOG_INFO("Loaded determined order book: {} bids, {} asks", bids.size(), asks.size());
    return true;
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Failed to load determined order book: {}", e.what());
    return false;
  }
}

bool MockExchange::LoadDeterminedOrderBookFromFile(const std::string& file_path) {
  try {
    std::ifstream file(file_path);
    if (!file.is_open()) {
      SPDLOG_ERROR("Failed to open fixture file: {}", file_path);
      return false;
    }
    
    std::stringstream buffer;
    buffer << file.rdbuf();
    return LoadDeterminedOrderBook(buffer.str());
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Failed to load fixture file {}: {}", file_path, e.what());
    return false;
  }
}

}  // namespace herm
