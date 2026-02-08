#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include "engine/market_data/order_book.hpp"

using namespace herm;

/**
 * @brief Unit tests for ExchangeOrderBook
 * 
 * Tests core order book functionality:
 * - Bid/ask insertion, updates, removal
 * - Best bid/ask retrieval
 * - Price level sorting (bids descending, asks ascending)
 * - Volume calculations
 * - Thread safety
 */
class OrderBookTest : public ::testing::Test {
protected:
  void SetUp() override {
    book_ = std::make_unique<ExchangeOrderBook>("BTCUSDT");
  }

  std::unique_ptr<ExchangeOrderBook> book_;
};

// ============================================================================
// Basic Functionality Tests
// ============================================================================

TEST_F(OrderBookTest, EmptyBook) {
  EXPECT_EQ(book_->GetSymbol(), "BTCUSDT");
  EXPECT_TRUE(book_->IsEmpty());
  
  double price, quantity;
  EXPECT_FALSE(book_->GetBestBid(price, quantity));
  EXPECT_FALSE(book_->GetBestAsk(price, quantity));
  
  EXPECT_TRUE(book_->GetBids().empty());
  EXPECT_TRUE(book_->GetAsks().empty());
}

TEST_F(OrderBookTest, UpdateBid) {
  book_->UpdateBid(50000.0, 1.5);
  
  double price, quantity;
  EXPECT_TRUE(book_->GetBestBid(price, quantity));
  EXPECT_DOUBLE_EQ(price, 50000.0);
  EXPECT_DOUBLE_EQ(quantity, 1.5);
  
  auto bids = book_->GetBids();
  ASSERT_EQ(bids.size(), 1);
  EXPECT_DOUBLE_EQ(bids[0].price, 50000.0);
  EXPECT_DOUBLE_EQ(bids[0].quantity, 1.5);
}

TEST_F(OrderBookTest, UpdateAsk) {
  book_->UpdateAsk(50100.0, 2.0);
  
  double price, quantity;
  EXPECT_TRUE(book_->GetBestAsk(price, quantity));
  EXPECT_DOUBLE_EQ(price, 50100.0);
  EXPECT_DOUBLE_EQ(quantity, 2.0);
  
  auto asks = book_->GetAsks();
  ASSERT_EQ(asks.size(), 1);
  EXPECT_DOUBLE_EQ(asks[0].price, 50100.0);
  EXPECT_DOUBLE_EQ(asks[0].quantity, 2.0);
}

// ============================================================================
// Sorting Tests
// ============================================================================

TEST_F(OrderBookTest, MultipleBidsSorted) {
  book_->UpdateBid(50000.0, 1.0);
  book_->UpdateBid(49900.0, 2.0);
  book_->UpdateBid(50100.0, 0.5);
  
  // Best bid should be highest price
  double price, quantity;
  EXPECT_TRUE(book_->GetBestBid(price, quantity));
  EXPECT_DOUBLE_EQ(price, 50100.0);
  EXPECT_DOUBLE_EQ(quantity, 0.5);
  
  auto bids = book_->GetBids();
  ASSERT_EQ(bids.size(), 3);
  // Should be sorted descending
  EXPECT_DOUBLE_EQ(bids[0].price, 50100.0);
  EXPECT_DOUBLE_EQ(bids[1].price, 50000.0);
  EXPECT_DOUBLE_EQ(bids[2].price, 49900.0);
}

TEST_F(OrderBookTest, MultipleAsksSorted) {
  book_->UpdateAsk(50100.0, 1.0);
  book_->UpdateAsk(50200.0, 2.0);
  book_->UpdateAsk(50000.0, 0.5);
  
  // Best ask should be lowest price
  double price, quantity;
  EXPECT_TRUE(book_->GetBestAsk(price, quantity));
  EXPECT_DOUBLE_EQ(price, 50000.0);
  EXPECT_DOUBLE_EQ(quantity, 0.5);
  
  auto asks = book_->GetAsks();
  ASSERT_EQ(asks.size(), 3);
  // Should be sorted ascending
  EXPECT_DOUBLE_EQ(asks[0].price, 50000.0);
  EXPECT_DOUBLE_EQ(asks[1].price, 50100.0);
  EXPECT_DOUBLE_EQ(asks[2].price, 50200.0);
}

// ============================================================================
// Update and Removal Tests
// ============================================================================

TEST_F(OrderBookTest, UpdateExistingLevel) {
  book_->UpdateBid(50000.0, 1.0);
  book_->UpdateBid(50000.0, 2.5);
  
  double price, quantity;
  EXPECT_TRUE(book_->GetBestBid(price, quantity));
  EXPECT_DOUBLE_EQ(price, 50000.0);
  EXPECT_DOUBLE_EQ(quantity, 2.5);
  
  auto bids = book_->GetBids();
  ASSERT_EQ(bids.size(), 1);
  EXPECT_DOUBLE_EQ(bids[0].quantity, 2.5);
}

TEST_F(OrderBookTest, RemoveBid) {
  book_->UpdateBid(50000.0, 1.0);
  book_->UpdateBid(49900.0, 2.0);
  
  book_->RemoveBid(50000.0);
  
  double price, quantity;
  EXPECT_TRUE(book_->GetBestBid(price, quantity));
  EXPECT_DOUBLE_EQ(price, 49900.0);
  
  auto bids = book_->GetBids();
  ASSERT_EQ(bids.size(), 1);
}

TEST_F(OrderBookTest, RemoveAsk) {
  book_->UpdateAsk(50100.0, 1.0);
  book_->UpdateAsk(50200.0, 2.0);
  
  book_->RemoveAsk(50100.0);
  
  double price, quantity;
  EXPECT_TRUE(book_->GetBestAsk(price, quantity));
  EXPECT_DOUBLE_EQ(price, 50200.0);
  
  auto asks = book_->GetAsks();
  ASSERT_EQ(asks.size(), 1);
}

TEST_F(OrderBookTest, ZeroQuantityRemoves) {
  book_->UpdateBid(50000.0, 1.0);
  book_->UpdateBid(50000.0, 0.0);  // Zero quantity removes
  
  EXPECT_TRUE(book_->IsEmpty());
  
  double price, quantity;
  EXPECT_FALSE(book_->GetBestBid(price, quantity));
}

TEST_F(OrderBookTest, Clear) {
  book_->UpdateBid(50000.0, 1.0);
  book_->UpdateAsk(50100.0, 2.0);
  
  EXPECT_FALSE(book_->IsEmpty());
  
  book_->Clear();
  
  EXPECT_TRUE(book_->IsEmpty());
}

// ============================================================================
// Depth Limiting Tests
// ============================================================================

TEST_F(OrderBookTest, GetDepthLimited) {
  book_->UpdateBid(50000.0, 1.0);
  book_->UpdateBid(49900.0, 2.0);
  book_->UpdateBid(49800.0, 3.0);
  
  auto bids = book_->GetBids(2);
  ASSERT_EQ(bids.size(), 2);
  EXPECT_DOUBLE_EQ(bids[0].price, 50000.0);
  EXPECT_DOUBLE_EQ(bids[1].price, 49900.0);
}

// ============================================================================
// Volume Calculation Tests
// ============================================================================

TEST_F(OrderBookTest, GetBidVolumeUpTo) {
  book_->UpdateBid(50000.0, 1.0);   // 50000 notional
  book_->UpdateBid(49900.0, 2.0);   // 99800 notional
  book_->UpdateBid(49800.0, 0.5);   // 24900 notional
  
  // Volume up to 50000 should include all bids <= 50000
  double volume = book_->GetBidVolumeUpTo(50000.0);
  EXPECT_NEAR(volume, 50000.0 + 99800.0 + 24900.0, 0.01);
  
  // Volume up to 49900 should include 49900 and 49800 (not 50000)
  volume = book_->GetBidVolumeUpTo(49900.0);
  EXPECT_NEAR(volume, 99800.0 + 24900.0, 0.01);
}

TEST_F(OrderBookTest, GetAskVolumeUpTo) {
  book_->UpdateAsk(50100.0, 1.0);   // 50100 notional
  book_->UpdateAsk(50200.0, 2.0);   // 100400 notional
  book_->UpdateAsk(50300.0, 0.5);   // 25150 notional
  
  // Volume from 50100 should include all asks
  double volume = book_->GetAskVolumeUpTo(50100.0);
  EXPECT_NEAR(volume, 50100.0 + 100400.0 + 25150.0, 0.01);
  
  // Volume from 50200 should include 50200 and 50300 (not 50100)
  volume = book_->GetAskVolumeUpTo(50200.0);
  EXPECT_NEAR(volume, 100400.0 + 25150.0, 0.01);
}

TEST_F(OrderBookTest, GetBidPriceForVolume) {
  book_->UpdateBid(50000.0, 1.0);   // 50000 notional
  book_->UpdateBid(49900.0, 2.0);   // 99800 notional (cumulative: 149800)
  book_->UpdateBid(49800.0, 0.5);   // 24900 notional (cumulative: 174700)
  
  double price;
  // 100000 notional should be at 49900 level
  EXPECT_TRUE(book_->GetBidPriceForVolume(100000.0, price));
  EXPECT_DOUBLE_EQ(price, 49900.0);
  
  // 150000 notional should be at 49800 level
  EXPECT_TRUE(book_->GetBidPriceForVolume(150000.0, price));
  EXPECT_DOUBLE_EQ(price, 49800.0);
  
  // 200000 notional should fail (not enough volume)
  EXPECT_FALSE(book_->GetBidPriceForVolume(200000.0, price));
}

TEST_F(OrderBookTest, GetAskPriceForVolume) {
  book_->UpdateAsk(50100.0, 1.0);   // 50100 notional
  book_->UpdateAsk(50200.0, 2.0);   // 100400 notional (cumulative: 150500)
  book_->UpdateAsk(50300.0, 0.5);   // 25150 notional (cumulative: 175650)
  
  double price;
  // 100000 notional should be at 50200 level
  EXPECT_TRUE(book_->GetAskPriceForVolume(100000.0, price));
  EXPECT_DOUBLE_EQ(price, 50200.0);
  
  // 150000 notional should be at 50200 level (cumulative: 150500 >= 150000)
  EXPECT_TRUE(book_->GetAskPriceForVolume(150000.0, price));
  EXPECT_DOUBLE_EQ(price, 50200.0);
  
  // 160000 notional should be at 50300 level
  EXPECT_TRUE(book_->GetAskPriceForVolume(160000.0, price));
  EXPECT_DOUBLE_EQ(price, 50300.0);
  
  // 200000 notional should fail (not enough volume)
  EXPECT_FALSE(book_->GetAskPriceForVolume(200000.0, price));
}

// ============================================================================
// Precision Tests
// ============================================================================

TEST_F(OrderBookTest, PricePrecision) {
  book_->UpdateBid(50000.123456, 1.0);
  book_->UpdateBid(49900.987654, 2.0);
  
  double price, qty;
  ASSERT_TRUE(book_->GetBestBid(price, qty));
  
  // Verify precision is maintained
  EXPECT_NEAR(price, 50000.123456, 0.000001);
}

// ============================================================================
// Thread Safety Tests
// ============================================================================

TEST_F(OrderBookTest, ConcurrentUpdates) {
  const int num_threads = 10;
  const int updates_per_thread = 100;
  
  std::vector<std::thread> threads;
  
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([this, i, updates_per_thread]() {
      for (int j = 0; j < updates_per_thread; ++j) {
        double price = 50000.0 + (i * 100.0) + (j * 0.1);
        double quantity = 1.0 + (j * 0.01);
        book_->UpdateBid(price, quantity);
        book_->UpdateAsk(price + 100.0, quantity);
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  // Should have many levels
  auto bids = book_->GetBids();
  auto asks = book_->GetAsks();
  EXPECT_GT(bids.size(), 0);
  EXPECT_GT(asks.size(), 0);
  
  // Best bid/ask should be valid
  double price, quantity;
  EXPECT_TRUE(book_->GetBestBid(price, quantity));
  EXPECT_TRUE(book_->GetBestAsk(price, quantity));
}
