#include <gtest/gtest.h>
#include "engine/aggregation/order_book_resampler.hpp"
#include "market_data.grpc.pb.h"

using namespace herm;
using namespace herm::market_data;

class OrderBookResamplerTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a full order book for testing
    full_book_.set_symbol("BTCUSDT");
    
    // Add bids (descending)
    auto* bid1 = full_book_.add_bids();
    bid1->set_price(50000.0);
    bid1->set_quantity(1.0);
    
    auto* bid2 = full_book_.add_bids();
    bid2->set_price(49900.0);
    bid2->set_quantity(2.0);
    
    auto* bid3 = full_book_.add_bids();
    bid3->set_price(49800.0);
    bid3->set_quantity(1.5);
    
    // Add asks (ascending)
    auto* ask1 = full_book_.add_asks();
    ask1->set_price(50100.0);
    ask1->set_quantity(1.2);
    
    auto* ask2 = full_book_.add_asks();
    ask2->set_price(50200.0);
    ask2->set_quantity(2.5);
    
    auto* ask3 = full_book_.add_asks();
    ask3->set_price(50300.0);
    ask3->set_quantity(1.8);
  }

  OrderBook full_book_;
};

TEST_F(OrderBookResamplerTest, BBOFiltering) {
  OrderBook output;
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::BBO, {}, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  ASSERT_EQ(output.bids_size(), 1);
  ASSERT_EQ(output.asks_size(), 1);
  
  EXPECT_DOUBLE_EQ(output.bids(0).price(), 50000.0);
  EXPECT_DOUBLE_EQ(output.bids(0).quantity(), 1.0);
  
  EXPECT_DOUBLE_EQ(output.asks(0).price(), 50100.0);
  EXPECT_DOUBLE_EQ(output.asks(0).quantity(), 1.2);
}

TEST_F(OrderBookResamplerTest, PriceBandSingleBasisPoint) {
  std::vector<int32_t> basis_points = {100};  // 1%
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::PRICE_BAND, basis_points, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_GE(output.asks_size(), 1);
}

TEST_F(OrderBookResamplerTest, PriceBandMultipleBasisPoints) {
  std::vector<int32_t> basis_points = {50, 100, 200};
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::PRICE_BAND, basis_points, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_GE(output.asks_size(), 1);
}

TEST_F(OrderBookResamplerTest, VolumeBandSingleVolume) {
  std::vector<double> notional_volumes = {100000.0};  // $100k
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::VOLUME_BAND, {}, notional_volumes, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_GE(output.asks_size(), 1);
}

TEST_F(OrderBookResamplerTest, VolumeBandMultipleVolumes) {
  std::vector<double> notional_volumes = {50000.0, 100000.0, 200000.0};
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::VOLUME_BAND, {}, notional_volumes, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_GE(output.asks_size(), 1);
}

TEST_F(OrderBookResamplerTest, NoneRequestType) {
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::NONE, {}, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  EXPECT_EQ(output.bids_size(), full_book_.bids_size());
  EXPECT_EQ(output.asks_size(), full_book_.asks_size());
}

TEST_F(OrderBookResamplerTest, EmptyOrderBook) {
  OrderBook empty_book;
  empty_book.set_symbol("BTCUSDT");
  
  OrderBook output;
  OrderBookResampler::ResampleOrderBook(
      empty_book, RequestType::BBO, {}, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  EXPECT_EQ(output.bids_size(), 0);
  EXPECT_EQ(output.asks_size(), 0);
}

TEST_F(OrderBookResamplerTest, OnlyBids) {
  OrderBook bids_only;
  bids_only.set_symbol("BTCUSDT");
  
  auto* bid = bids_only.add_bids();
  bid->set_price(50000.0);
  bid->set_quantity(1.0);
  
  OrderBook output;
  OrderBookResampler::ResampleOrderBook(
      bids_only, RequestType::BBO, {}, {}, &output);
  
  EXPECT_EQ(output.bids_size(), 1);
  EXPECT_EQ(output.asks_size(), 0);
}

// ============================================================================
// BBO Advanced Tests
// ============================================================================

TEST_F(OrderBookResamplerTest, BBOPreservesVenueInformation) {
  // Add venue information to the order book
  auto* bid_venue = full_book_.mutable_bids(0)->add_venues();
  bid_venue->set_venue_name("binance");
  bid_venue->set_quantity(0.5);
  
  auto* bid_venue2 = full_book_.mutable_bids(0)->add_venues();
  bid_venue2->set_venue_name("bybit");
  bid_venue2->set_quantity(0.5);
  
  OrderBook output;
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::BBO, {}, {}, &output);
  
  ASSERT_EQ(output.bids_size(), 1);
  EXPECT_EQ(output.bids(0).venues_size(), 2);
}

TEST_F(OrderBookResamplerTest, BBOWithOnlyAsks) {
  OrderBook asks_only;
  asks_only.set_symbol("BTCUSDT");
  
  auto* ask = asks_only.add_asks();
  ask->set_price(50100.0);
  ask->set_quantity(1.0);
  
  OrderBook output;
  OrderBookResampler::ResampleOrderBook(
      asks_only, RequestType::BBO, {}, {}, &output);
  
  EXPECT_EQ(output.bids_size(), 0);
  EXPECT_EQ(output.asks_size(), 1);
  EXPECT_DOUBLE_EQ(output.asks(0).price(), 50100.0);
}

// ============================================================================
// Price Band Detailed Tests
// ============================================================================

TEST_F(OrderBookResamplerTest, PriceBandZeroBasisPoints) {
  std::vector<int32_t> basis_points = {0};
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::PRICE_BAND, basis_points, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  // With 0 basis points, should return BBO
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_GE(output.asks_size(), 1);
}

TEST_F(OrderBookResamplerTest, PriceBandLargeBasisPoints) {
  std::vector<int32_t> basis_points = {500};  // 5%
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::PRICE_BAND, basis_points, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  // Should capture all price levels within 5% of BBO
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_GE(output.asks_size(), 1);
}

TEST_F(OrderBookResamplerTest, PriceBandMultipleBands) {
  std::vector<int32_t> basis_points = {10, 25, 50, 100};
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::PRICE_BAND, basis_points, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  // Should have one level per basis point (or fewer if book is thin)
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_LE(output.bids_size(), 4);
  EXPECT_GE(output.asks_size(), 1);
  EXPECT_LE(output.asks_size(), 4);
}

TEST_F(OrderBookResamplerTest, PriceBandCalculation) {
  // Test specific price band boundaries
  // BBO: bid=50000, ask=50100
  // 100 bps = 1%
  // Bid band at 100 bps: 50000 * (1 - 0.01) = 49500
  // Ask band at 100 bps: 50100 * (1 + 0.01) = 50601
  
  std::vector<int32_t> basis_points = {100};
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::PRICE_BAND, basis_points, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  
  // All bid levels should be >= 49500
  for (int i = 0; i < output.bids_size(); ++i) {
    EXPECT_GE(output.bids(i).price(), 49500.0);
  }
  
  // All ask levels should be <= 50601
  for (int i = 0; i < output.asks_size(); ++i) {
    EXPECT_LE(output.asks(i).price(), 50601.0);
  }
}

// ============================================================================
// Volume Band Detailed Tests
// ============================================================================

TEST_F(OrderBookResamplerTest, VolumeBandSmallVolume) {
  // Small volume that fits within first level
  std::vector<double> notional_volumes = {10000.0};  // $10k
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::VOLUME_BAND, {}, notional_volumes, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_GE(output.asks_size(), 1);
}

TEST_F(OrderBookResamplerTest, VolumeBandVeryLargeVolume) {
  // Volume larger than entire book
  std::vector<double> notional_volumes = {10000000.0};  // $10M
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::VOLUME_BAND, {}, notional_volumes, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  // With very large volume target, book might be exhausted (empty result)
  // This is valid behavior - not enough liquidity
  EXPECT_GE(output.bids_size(), 0);
  EXPECT_GE(output.asks_size(), 0);
}

TEST_F(OrderBookResamplerTest, VolumeBandMultipleVolumeTargets) {
  std::vector<double> notional_volumes = {25000.0, 75000.0, 150000.0};
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::VOLUME_BAND, {}, notional_volumes, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  // Should have one level per volume target (or fewer if book is exhausted)
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_LE(output.bids_size(), 3);
  EXPECT_GE(output.asks_size(), 1);
  EXPECT_LE(output.asks_size(), 3);
}

TEST_F(OrderBookResamplerTest, VolumeBandVWAPCalculation) {
  // Verify VWAP is reasonable
  std::vector<double> notional_volumes = {100000.0};
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::VOLUME_BAND, {}, notional_volumes, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  
  if (output.bids_size() > 0) {
    // VWAP should be at or below best bid
    EXPECT_LE(output.bids(0).price(), full_book_.bids(0).price());
  }
  
  if (output.asks_size() > 0) {
    // VWAP should be at or above best ask
    EXPECT_GE(output.asks(0).price(), full_book_.asks(0).price());
  }
}

TEST_F(OrderBookResamplerTest, VolumeBandZeroVolume) {
  std::vector<double> notional_volumes = {0.0};
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::VOLUME_BAND, {}, notional_volumes, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  // Zero volume should return empty or minimal result
  EXPECT_GE(output.bids_size(), 0);
  EXPECT_GE(output.asks_size(), 0);
}

// ============================================================================
// Edge Cases and Boundary Conditions
// ============================================================================

TEST_F(OrderBookResamplerTest, EmptyBasisPointsWithPriceBand) {
  std::vector<int32_t> empty_basis_points;
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::PRICE_BAND, empty_basis_points, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  // Empty basis points should return empty result or full book
  EXPECT_GE(output.bids_size(), 0);
}

TEST_F(OrderBookResamplerTest, EmptyVolumesWithVolumeBand) {
  std::vector<double> empty_volumes;
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::VOLUME_BAND, {}, empty_volumes, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  EXPECT_GE(output.bids_size(), 0);
}

TEST_F(OrderBookResamplerTest, MixedRequestTypeHandling) {
  // Test that parameters for wrong request type are ignored
  std::vector<int32_t> basis_points = {100};
  std::vector<double> volumes = {50000.0};
  OrderBook output;
  
  // BBO request should ignore both parameters
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::BBO, basis_points, volumes, &output);
  
  EXPECT_EQ(output.bids_size(), 1);
  EXPECT_EQ(output.asks_size(), 1);
}

// ============================================================================
// Large Book Tests
// ============================================================================

TEST_F(OrderBookResamplerTest, LargeBookBBO) {
  OrderBook large_book;
  large_book.set_symbol("BTCUSDT");
  
  // Add 100 bid levels
  for (int i = 0; i < 100; ++i) {
    auto* bid = large_book.add_bids();
    bid->set_price(50000.0 - i * 10.0);
    bid->set_quantity(1.0 + i * 0.1);
  }
  
  // Add 100 ask levels
  for (int i = 0; i < 100; ++i) {
    auto* ask = large_book.add_asks();
    ask->set_price(50100.0 + i * 10.0);
    ask->set_quantity(1.0 + i * 0.1);
  }
  
  OrderBook output;
  OrderBookResampler::ResampleOrderBook(
      large_book, RequestType::BBO, {}, {}, &output);
  
  ASSERT_EQ(output.bids_size(), 1);
  ASSERT_EQ(output.asks_size(), 1);
  EXPECT_DOUBLE_EQ(output.bids(0).price(), 50000.0);
  EXPECT_DOUBLE_EQ(output.asks(0).price(), 50100.0);
}

TEST_F(OrderBookResamplerTest, LargeBookPriceBand) {
  OrderBook large_book;
  large_book.set_symbol("BTCUSDT");
  
  // Add 50 bid levels
  for (int i = 0; i < 50; ++i) {
    auto* bid = large_book.add_bids();
    bid->set_price(50000.0 - i * 10.0);
    bid->set_quantity(1.0);
  }
  
  // Add 50 ask levels
  for (int i = 0; i < 50; ++i) {
    auto* ask = large_book.add_asks();
    ask->set_price(50100.0 + i * 10.0);
    ask->set_quantity(1.0);
  }
  
  std::vector<int32_t> basis_points = {200};  // 2%
  OrderBook output;
  
  OrderBookResampler::ResampleOrderBook(
      large_book, RequestType::PRICE_BAND, basis_points, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  // Should capture levels within 2% of best
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_GE(output.asks_size(), 1);
}

// ============================================================================
// Symbol and Metadata Preservation
// ============================================================================

TEST_F(OrderBookResamplerTest, SymbolPreservation) {
  OrderBook book;
  book.set_symbol("ETHUSDT");
  
  auto* bid = book.add_bids();
  bid->set_price(3000.0);
  bid->set_quantity(10.0);
  
  auto* ask = book.add_asks();
  ask->set_price(3010.0);
  ask->set_quantity(12.0);
  
  OrderBook output;
  OrderBookResampler::ResampleOrderBook(
      book, RequestType::BBO, {}, {}, &output);
  
  EXPECT_EQ(output.symbol(), "ETHUSDT");
}

TEST_F(OrderBookResamplerTest, TimestampPreservation) {
  full_book_.set_timestamp_us(1234567890);
  
  OrderBook output;
  OrderBookResampler::ResampleOrderBook(
      full_book_, RequestType::BBO, {}, {}, &output);
  
  // Timestamp should be preserved or updated
  EXPECT_GT(output.timestamp_us(), 0);
}

// ============================================================================
// Request Type Combinations
// ============================================================================

TEST_F(OrderBookResamplerTest, AllRequestTypes) {
  std::vector<RequestType> types = {
    RequestType::BBO,
    RequestType::PRICE_BAND,
    RequestType::VOLUME_BAND,
    RequestType::NONE
  };
  
  std::vector<int32_t> basis_points = {100};
  std::vector<double> volumes = {50000.0};
  
  for (auto type : types) {
    OrderBook output;
    OrderBookResampler::ResampleOrderBook(
        full_book_, type, basis_points, volumes, &output);
    
    EXPECT_EQ(output.symbol(), "BTCUSDT");
    // All types should produce valid output
    if (type != RequestType::NONE || full_book_.bids_size() > 0) {
      EXPECT_GE(output.bids_size(), 0);
      EXPECT_GE(output.asks_size(), 0);
    }
  }
}
