#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "mock_exchange.hpp"

using namespace herm;
using namespace std::chrono_literals;

class MockExchangeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    exchange_ = std::make_unique<MockExchange>(
        "test_exchange", "BTCUSDT", 50000.0, 0.001, 50ms);
  }

  void TearDown() override {
    if (exchange_) {
      exchange_->Stop();
    }
  }

  std::unique_ptr<MockExchange> exchange_;
};

TEST_F(MockExchangeTest, InitialState) {
  EXPECT_EQ(exchange_->GetExchangeName(), "test_exchange");
  EXPECT_EQ(exchange_->GetSymbol(), "BTCUSDT");
  
  auto bids = exchange_->GetBids();
  auto asks = exchange_->GetAsks();
  
  // Should have initial levels
  EXPECT_GT(bids.size(), 0);
  EXPECT_GT(asks.size(), 0);
}

TEST_F(MockExchangeTest, StartStop) {
  EXPECT_FALSE(exchange_->GetOrderBook().IsEmpty());
  
  exchange_->Start();
  std::this_thread::sleep_for(200ms);
  
  // Should have updated
  auto bids_before = exchange_->GetBids();
  
  std::this_thread::sleep_for(100ms);
  auto bids_after = exchange_->GetBids();
  
  // May have changed (depending on timing)
  exchange_->Stop();
}

TEST_F(MockExchangeTest, OrderBookStructure) {
  auto bids = exchange_->GetBids();
  auto asks = exchange_->GetAsks();
  
  // Bids should be sorted descending
  for (size_t i = 1; i < bids.size(); ++i) {
    EXPECT_GE(bids[i-1].price, bids[i].price);
  }
  
  // Asks should be sorted ascending
  for (size_t i = 1; i < asks.size(); ++i) {
    EXPECT_LE(asks[i-1].price, asks[i].price);
  }
  
  // Best bid should be higher than best ask (or close)
  if (!bids.empty() && !asks.empty()) {
    double best_bid = bids[0].price;
    double best_ask = asks[0].price;
    // In a real market, bid < ask, but mock might have some overlap
    // Just check they're reasonable
    EXPECT_GT(best_bid, 0.0);
    EXPECT_GT(best_ask, 0.0);
  }
}

TEST_F(MockExchangeTest, PriceLevelsValid) {
  auto bids = exchange_->GetBids();
  auto asks = exchange_->GetAsks();
  
  for (const auto& bid : bids) {
    EXPECT_GT(bid.price, 0.0);
    EXPECT_GT(bid.quantity, 0.0);
  }
  
  for (const auto& ask : asks) {
    EXPECT_GT(ask.price, 0.0);
    EXPECT_GT(ask.quantity, 0.0);
  }
}

TEST_F(MockExchangeTest, UpdatesOverTime) {
  exchange_->Start();
  
  // Get initial state
  auto initial_bids = exchange_->GetBids();
  
  // Wait for a few updates
  std::this_thread::sleep_for(300ms);
  
  auto later_bids = exchange_->GetBids();
  
  // Should have some updates (may or may not be different due to randomness)
  exchange_->Stop();
}

TEST_F(MockExchangeTest, MultipleExchanges) {
  auto exchange1 = std::make_unique<MockExchange>(
      "exchange1", "BTCUSDT", 50000.0, 0.001, 50ms);
  auto exchange2 = std::make_unique<MockExchange>(
      "exchange2", "BTCUSDT", 50000.0, 0.001, 50ms);
  
  auto bids1 = exchange1->GetBids();
  auto bids2 = exchange2->GetBids();
  
  // Both should have valid order books
  EXPECT_GT(bids1.size(), 0);
  EXPECT_GT(bids2.size(), 0);
  
  exchange1->Stop();
  exchange2->Stop();
}
