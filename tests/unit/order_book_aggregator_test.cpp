#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <set>
#include <chrono>
#include "engine/market_data/order_book_aggregator.hpp"

using namespace herm;

class OrderBookAggregatorTest : public ::testing::Test {
protected:
  void SetUp() override {
    aggregator_ = std::make_unique<OrderBookAggregator>("BTCUSDT");
  }

  std::unique_ptr<OrderBookAggregator> aggregator_;
};

TEST_F(OrderBookAggregatorTest, EmptyAggregator) {
  EXPECT_EQ(aggregator_->GetSymbol(), "BTCUSDT");
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  EXPECT_EQ(pooled_book->symbol(), "BTCUSDT");
  EXPECT_EQ(pooled_book->bids_size(), 0);
  EXPECT_EQ(pooled_book->asks_size(), 0);
}

TEST_F(OrderBookAggregatorTest, SingleExchange) {
  std::vector<PriceLevel> bids = {{50000.0, 1.0}, {49900.0, 2.0}};
  std::vector<PriceLevel> asks = {{50100.0, 1.5}, {50200.0, 2.5}};
  
  aggregator_->UpdateExchangeBook("binance", bids, asks);
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  ASSERT_EQ(pooled_book->bids_size(), 2);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).price(), 50000.0);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).quantity(), 1.0);
  
  ASSERT_EQ(pooled_book->asks_size(), 2);
  EXPECT_DOUBLE_EQ(pooled_book->asks(0).price(), 50100.0);
  EXPECT_DOUBLE_EQ(pooled_book->asks(0).quantity(), 1.5);
  
  EXPECT_EQ(pooled_book->bids(0).venues_size(), 1);
  EXPECT_EQ(pooled_book->bids(0).venues(0).venue_name(), "binance");
}

TEST_F(OrderBookAggregatorTest, MultipleExchangesSamePrice) {
  std::vector<PriceLevel> binance_bids = {{50000.0, 1.0}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  std::vector<PriceLevel> bybit_bids = {{50000.0, 2.0}};
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, empty_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  ASSERT_EQ(pooled_book->bids_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).price(), 50000.0);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).quantity(), 3.0);
  
  ASSERT_EQ(pooled_book->bids(0).venues_size(), 2);
  std::set<std::string> venue_names;
  for (int i = 0; i < pooled_book->bids(0).venues_size(); ++i) {
    venue_names.insert(pooled_book->bids(0).venues(i).venue_name());
  }
  EXPECT_EQ(venue_names.size(), 2);
}

TEST_F(OrderBookAggregatorTest, VenueContributions) {
  std::vector<PriceLevel> binance_bids = {{50000.0, 1.5}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  auto contributions = aggregator_->GetVenueContributions(50000.0, true);
  ASSERT_EQ(contributions.size(), 1);
  EXPECT_EQ(contributions[0].venue_name, "binance");
  EXPECT_DOUBLE_EQ(contributions[0].quantity, 1.5);
}

TEST_F(OrderBookAggregatorTest, VenueTimestamps) {
  auto before = std::chrono::system_clock::now();
  
  std::vector<PriceLevel> binance_bids = {{50000.0, 1.5}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  auto after = std::chrono::system_clock::now();
  auto timestamp = aggregator_->GetVenueTimestamp("binance");
  
  EXPECT_GE(timestamp, before);
  EXPECT_LE(timestamp, after);
}

TEST_F(OrderBookAggregatorTest, Clear) {
  std::vector<PriceLevel> binance_bids = {{50000.0, 1.0}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  auto pooled_book_before = aggregator_->GetPooledAggregatedOrderBook();
  EXPECT_GT(pooled_book_before->bids_size(), 0);
  
  aggregator_->Clear();
  
  auto pooled_book_after = aggregator_->GetPooledAggregatedOrderBook();
  EXPECT_EQ(pooled_book_after->bids_size(), 0);
}

TEST_F(OrderBookAggregatorTest, ConcurrentUpdates) {
  const int num_threads = 5;
  const int updates_per_thread = 50;
  
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    std::string exchange = "exchange" + std::to_string(i);
    threads.emplace_back([this, exchange, updates_per_thread]() {
      for (int j = 0; j < updates_per_thread; ++j) {
        double price = 50000.0 + (j * 0.1);
        double quantity = 1.0 + (j * 0.01);
        std::vector<PriceLevel> bids = {{price, quantity}};
        std::vector<PriceLevel> asks = {{price + 100.0, quantity}};
        aggregator_->UpdateExchangeBook(exchange, bids, asks);
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  EXPECT_GT(pooled_book->bids_size(), 0);
  EXPECT_GT(pooled_book->asks_size(), 0);
}

// ============================================================================
// Venue Update and Replacement Tests
// ============================================================================

TEST_F(OrderBookAggregatorTest, VenueReplacement) {
  // Initial update from binance
  std::vector<PriceLevel> binance_bids1 = {{50000.0, 1.0}, {49900.0, 2.0}};
  std::vector<PriceLevel> binance_asks1;
  aggregator_->UpdateExchangeBook("binance", binance_bids1, binance_asks1);
  
  auto pooled_book1 = aggregator_->GetPooledAggregatedOrderBook();
  ASSERT_EQ(pooled_book1->bids_size(), 2);
  
  // Replace with new prices
  std::vector<PriceLevel> binance_bids2 = {{50100.0, 0.5}};
  std::vector<PriceLevel> binance_asks2;
  aggregator_->UpdateExchangeBook("binance", binance_bids2, binance_asks2);
  
  auto pooled_book2 = aggregator_->GetPooledAggregatedOrderBook();
  ASSERT_EQ(pooled_book2->bids_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book2->bids(0).price(), 50100.0);
  EXPECT_DOUBLE_EQ(pooled_book2->bids(0).quantity(), 0.5);
}

TEST_F(OrderBookAggregatorTest, ThreeVenuesSamePrice) {
  std::vector<PriceLevel> binance_bids = {{50000.0, 1.0}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  std::vector<PriceLevel> bybit_bids = {{50000.0, 2.0}};
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, empty_asks);
  
  std::vector<PriceLevel> okx_bids = {{50000.0, 1.5}};
  aggregator_->UpdateExchangeBook("okx", okx_bids, empty_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  ASSERT_EQ(pooled_book->bids_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).price(), 50000.0);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).quantity(), 4.5);  // 1.0 + 2.0 + 1.5
  
  ASSERT_EQ(pooled_book->bids(0).venues_size(), 3);
}

TEST_F(OrderBookAggregatorTest, VenueRemovalByEmptyUpdate) {
  // Add from binance
  std::vector<PriceLevel> binance_bids = {{50000.0, 1.0}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  // Add from bybit
  std::vector<PriceLevel> bybit_bids = {{50000.0, 2.0}};
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, empty_asks);
  
  auto pooled_book1 = aggregator_->GetPooledAggregatedOrderBook();
  EXPECT_DOUBLE_EQ(pooled_book1->bids(0).quantity(), 3.0);
  
  // Remove binance by sending empty update
  std::vector<PriceLevel> empty_bids;
  aggregator_->UpdateExchangeBook("binance", empty_bids, empty_asks);
  
  auto pooled_book2 = aggregator_->GetPooledAggregatedOrderBook();
  ASSERT_EQ(pooled_book2->bids_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book2->bids(0).quantity(), 2.0);  // Only bybit remains
  ASSERT_EQ(pooled_book2->bids(0).venues_size(), 1);
  EXPECT_EQ(pooled_book2->bids(0).venues(0).venue_name(), "bybit");
}

// ============================================================================
// Large Order Book Tests
// ============================================================================

TEST_F(OrderBookAggregatorTest, LargeOrderBook) {
  std::vector<PriceLevel> binance_bids;
  std::vector<PriceLevel> binance_asks;
  
  // Add 100 bid levels
  for (int i = 0; i < 100; ++i) {
    binance_bids.push_back({50000.0 - i * 10.0, 1.0 + i * 0.1});
  }
  
  // Add 100 ask levels
  for (int i = 0; i < 100; ++i) {
    binance_asks.push_back({50100.0 + i * 10.0, 1.0 + i * 0.1});
  }
  
  aggregator_->UpdateExchangeBook("binance", binance_bids, binance_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  EXPECT_EQ(pooled_book->bids_size(), 100);
  EXPECT_EQ(pooled_book->asks_size(), 100);
  
  // Verify sorting
  for (int i = 0; i < 99; ++i) {
    EXPECT_GT(pooled_book->bids(i).price(), pooled_book->bids(i + 1).price());
    EXPECT_LT(pooled_book->asks(i).price(), pooled_book->asks(i + 1).price());
  }
}

TEST_F(OrderBookAggregatorTest, InterleavedPrices) {
  // Binance: 50100, 50000, 49900
  std::vector<PriceLevel> binance_bids = {{50100.0, 1.0}, {50000.0, 1.5}, {49900.0, 2.0}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  // Bybit: 50050, 49950, 49850 (interleaved)
  std::vector<PriceLevel> bybit_bids = {{50050.0, 1.2}, {49950.0, 1.8}, {49850.0, 2.5}};
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, empty_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  // Should be sorted: 50100, 50050, 50000, 49950, 49900, 49850
  ASSERT_EQ(pooled_book->bids_size(), 6);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).price(), 50100.0);
  EXPECT_DOUBLE_EQ(pooled_book->bids(1).price(), 50050.0);
  EXPECT_DOUBLE_EQ(pooled_book->bids(2).price(), 50000.0);
  EXPECT_DOUBLE_EQ(pooled_book->bids(3).price(), 49950.0);
  EXPECT_DOUBLE_EQ(pooled_book->bids(4).price(), 49900.0);
  EXPECT_DOUBLE_EQ(pooled_book->bids(5).price(), 49850.0);
}

// ============================================================================
// Price Precision Tests
// ============================================================================

TEST_F(OrderBookAggregatorTest, HighPrecisionPrices) {
  std::vector<PriceLevel> binance_bids = {{50000.123456, 1.0}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  std::vector<PriceLevel> bybit_bids = {{50000.123456, 2.0}};  // Same price
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, empty_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  ASSERT_EQ(pooled_book->bids_size(), 1);
  EXPECT_NEAR(pooled_book->bids(0).price(), 50000.123456, 0.000001);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).quantity(), 3.0);
}

TEST_F(OrderBookAggregatorTest, VeryCloseButDifferentPrices) {
  std::vector<PriceLevel> binance_bids = {{50000.10, 1.0}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  std::vector<PriceLevel> bybit_bids = {{50000.11, 2.0}};  // Slightly different
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, empty_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  // Should be 2 separate price levels
  ASSERT_EQ(pooled_book->bids_size(), 2);
  EXPECT_NEAR(pooled_book->bids(0).price(), 50000.11, 0.001);
  EXPECT_NEAR(pooled_book->bids(1).price(), 50000.10, 0.001);
}

// ============================================================================
// Asymmetric Book Tests
// ============================================================================

TEST_F(OrderBookAggregatorTest, BidsOnlyFromMultipleVenues) {
  std::vector<PriceLevel> binance_bids = {{50000.0, 1.0}, {49900.0, 2.0}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  std::vector<PriceLevel> bybit_bids = {{50050.0, 1.5}};
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, empty_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  EXPECT_EQ(pooled_book->bids_size(), 3);
  EXPECT_EQ(pooled_book->asks_size(), 0);
}

TEST_F(OrderBookAggregatorTest, AsksOnlyFromMultipleVenues) {
  std::vector<PriceLevel> empty_bids;
  std::vector<PriceLevel> binance_asks = {{50100.0, 1.0}, {50200.0, 2.0}};
  aggregator_->UpdateExchangeBook("binance", empty_bids, binance_asks);
  
  std::vector<PriceLevel> bybit_asks = {{50150.0, 1.5}};
  aggregator_->UpdateExchangeBook("bybit", empty_bids, bybit_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  EXPECT_EQ(pooled_book->bids_size(), 0);
  EXPECT_EQ(pooled_book->asks_size(), 3);
}

// ============================================================================
// Stress Tests
// ============================================================================

TEST_F(OrderBookAggregatorTest, ManyVenuesManySamePrices) {
  std::vector<PriceLevel> bids = {{50000.0, 1.0}};
  std::vector<PriceLevel> empty_asks;
  
  // 10 venues all with same price
  for (int i = 0; i < 10; ++i) {
    std::string venue = "venue" + std::to_string(i);
    aggregator_->UpdateExchangeBook(venue, bids, empty_asks);
  }
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  ASSERT_EQ(pooled_book->bids_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).price(), 50000.0);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).quantity(), 10.0);
  EXPECT_EQ(pooled_book->bids(0).venues_size(), 10);
}

TEST_F(OrderBookAggregatorTest, RapidUpdates) {
  for (int i = 0; i < 100; ++i) {
    std::vector<PriceLevel> bids = {{50000.0 + i * 0.01, 1.0}};
    std::vector<PriceLevel> empty_asks;
    aggregator_->UpdateExchangeBook("binance", bids, empty_asks);
    
    auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
    EXPECT_EQ(pooled_book->bids_size(), 1);
  }
}

// ============================================================================
// Venue Isolation Tests
// ============================================================================

TEST_F(OrderBookAggregatorTest, VenueIsolationInAggregation) {
  // Each venue should maintain independent contribution
  std::vector<PriceLevel> binance_bids = {{50000.0, 1.0}, {49900.0, 2.0}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  std::vector<PriceLevel> bybit_bids = {{50000.0, 0.5}, {49950.0, 1.5}};
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, empty_asks);
  
  // Check 50000.0 level
  auto contributions_50000 = aggregator_->GetVenueContributions(50000.0, true);
  ASSERT_EQ(contributions_50000.size(), 2);
  
  double binance_qty = 0.0, bybit_qty = 0.0;
  for (const auto& contrib : contributions_50000) {
    if (contrib.venue_name == "binance") binance_qty = contrib.quantity;
    if (contrib.venue_name == "bybit") bybit_qty = contrib.quantity;
  }
  
  EXPECT_DOUBLE_EQ(binance_qty, 1.0);
  EXPECT_DOUBLE_EQ(bybit_qty, 0.5);
  
  // Check 49900.0 level (only binance)
  auto contributions_49900 = aggregator_->GetVenueContributions(49900.0, true);
  ASSERT_EQ(contributions_49900.size(), 1);
  EXPECT_EQ(contributions_49900[0].venue_name, "binance");
  EXPECT_DOUBLE_EQ(contributions_49900[0].quantity, 2.0);
}

// ============================================================================
// Quantity Edge Cases
// ============================================================================

TEST_F(OrderBookAggregatorTest, VerySmallQuantities) {
  std::vector<PriceLevel> binance_bids = {{50000.0, 0.000001}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  std::vector<PriceLevel> bybit_bids = {{50000.0, 0.000002}};
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, empty_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  ASSERT_EQ(pooled_book->bids_size(), 1);
  EXPECT_NEAR(pooled_book->bids(0).quantity(), 0.000003, 0.0000001);
}

TEST_F(OrderBookAggregatorTest, VeryLargeQuantities) {
  std::vector<PriceLevel> binance_bids = {{50000.0, 1000000.0}};
  std::vector<PriceLevel> empty_asks;
  aggregator_->UpdateExchangeBook("binance", binance_bids, empty_asks);
  
  std::vector<PriceLevel> bybit_bids = {{50000.0, 2000000.0}};
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, empty_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  ASSERT_EQ(pooled_book->bids_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).quantity(), 3000000.0);
}

// ============================================================================
// Cross-Exchange Arbitrage Tests
// ============================================================================

TEST_F(OrderBookAggregatorTest, CrossExchangeArbitrage_BidGreaterThanAsk) {
  // Test cross-exchange arbitrage scenario where aggregated best bid > best ask
  // This is a VALID scenario representing arbitrage opportunity across venues
  
  // Binance: High bid (willing to buy at high price)
  std::vector<PriceLevel> binance_bids = {{50200.0, 10.0}};
  std::vector<PriceLevel> binance_asks = {{50300.0, 5.0}};
  
  // OKX: Low ask (willing to sell at low price)
  std::vector<PriceLevel> okx_bids = {{50000.0, 8.0}};
  std::vector<PriceLevel> okx_asks = {{50100.0, 6.0}};
  
  aggregator_->UpdateExchangeBook("binance", binance_bids, binance_asks);
  aggregator_->UpdateExchangeBook("okx", okx_bids, okx_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  // Best bid should be from Binance (50200)
  ASSERT_GE(pooled_book->bids_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).price(), 50200.0);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).quantity(), 10.0);
  
  // Best ask should be from OKX (50100)
  ASSERT_GE(pooled_book->asks_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->asks(0).price(), 50100.0);
  EXPECT_DOUBLE_EQ(pooled_book->asks(0).quantity(), 6.0);
  
  // Verify arbitrage: best bid > best ask (VALID for cross-exchange)
  EXPECT_GT(pooled_book->bids(0).price(), pooled_book->asks(0).price());
  
  // Arbitrage profit calculation
  double arbitrage_profit = pooled_book->bids(0).price() - pooled_book->asks(0).price();
  EXPECT_DOUBLE_EQ(arbitrage_profit, 100.0);  // Buy OKX @ 50100, sell Binance @ 50200
}

TEST_F(OrderBookAggregatorTest, CrossExchangeArbitrage_VenueTracking) {
  // Verify venue contributions are tracked correctly in arbitrage scenario
  
  std::vector<PriceLevel> binance_bids = {{50200.0, 10.0}};
  std::vector<PriceLevel> binance_asks = {{50110.0, 5.0}};
  
  std::vector<PriceLevel> bybit_bids = {{50100.0, 8.0}};
  std::vector<PriceLevel> bybit_asks = {{50105.0, 6.0}};
  
  aggregator_->UpdateExchangeBook("binance", binance_bids, binance_asks);
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, bybit_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  // Best bid from Binance
  ASSERT_GE(pooled_book->bids_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).price(), 50200.0);
  ASSERT_GE(pooled_book->bids(0).venues_size(), 1);
  EXPECT_EQ(pooled_book->bids(0).venues(0).venue_name(), "binance");
  
  // Best ask from Bybit  
  ASSERT_GE(pooled_book->asks_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->asks(0).price(), 50105.0);
  ASSERT_GE(pooled_book->asks(0).venues_size(), 1);
  EXPECT_EQ(pooled_book->asks(0).venues(0).venue_name(), "bybit");
  
  // Arbitrage exists
  EXPECT_GT(pooled_book->bids(0).price(), pooled_book->asks(0).price());
}

TEST_F(OrderBookAggregatorTest, NoArbitrage_NormalSpread) {
  // Test normal scenario with no arbitrage (best bid < best ask)
  
  std::vector<PriceLevel> binance_bids = {{50100.0, 10.0}};
  std::vector<PriceLevel> binance_asks = {{50105.0, 5.0}};
  
  std::vector<PriceLevel> okx_bids = {{50098.0, 8.0}};
  std::vector<PriceLevel> okx_asks = {{50103.0, 6.0}};
  
  aggregator_->UpdateExchangeBook("binance", binance_bids, binance_asks);
  aggregator_->UpdateExchangeBook("okx", okx_bids, okx_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  // Best bid should be from Binance (50100)
  ASSERT_GE(pooled_book->bids_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).price(), 50100.0);
  
  // Best ask should be from OKX (50103)
  ASSERT_GE(pooled_book->asks_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->asks(0).price(), 50103.0);
  
  // Verify no arbitrage: best bid < best ask
  EXPECT_LT(pooled_book->bids(0).price(), pooled_book->asks(0).price());
  
  // Positive spread
  double spread = pooled_book->asks(0).price() - pooled_book->bids(0).price();
  EXPECT_DOUBLE_EQ(spread, 3.0);
}

TEST_F(OrderBookAggregatorTest, CrossExchangeArbitrage_MultipleVenues) {
  // Test arbitrage with 3 venues
  
  std::vector<PriceLevel> binance_bids = {{50200.0, 10.0}, {50199.0, 15.0}};
  std::vector<PriceLevel> binance_asks = {{50300.0, 5.0}};
  
  std::vector<PriceLevel> okx_bids = {{50150.0, 8.0}};
  std::vector<PriceLevel> okx_asks = {{50151.0, 6.0}};
  
  std::vector<PriceLevel> bybit_bids = {{50100.0, 12.0}};
  std::vector<PriceLevel> bybit_asks = {{50101.0, 7.0}};  // Best ask across all venues
  
  aggregator_->UpdateExchangeBook("binance", binance_bids, binance_asks);
  aggregator_->UpdateExchangeBook("okx", okx_bids, okx_asks);
  aggregator_->UpdateExchangeBook("bybit", bybit_bids, bybit_asks);
  
  auto pooled_book = aggregator_->GetPooledAggregatedOrderBook();
  
  // Best bid: Binance @ 50200
  ASSERT_GE(pooled_book->bids_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->bids(0).price(), 50200.0);
  
  // Best ask: Bybit @ 50101
  ASSERT_GE(pooled_book->asks_size(), 1);
  EXPECT_DOUBLE_EQ(pooled_book->asks(0).price(), 50101.0);
  
  // Large arbitrage opportunity
  EXPECT_GT(pooled_book->bids(0).price(), pooled_book->asks(0).price());
  double arbitrage = pooled_book->bids(0).price() - pooled_book->asks(0).price();
  EXPECT_DOUBLE_EQ(arbitrage, 99.0);
}

