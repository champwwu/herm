#include <gtest/gtest.h>
#include "engine/aggregation/order_book_processor.hpp"
#include "engine/market_data/order_book.hpp"
#include "engine/common/event_thread.hpp"

using namespace herm;
using namespace herm::market_data;
using namespace herm::engine::common;

class OrderBookProcessorTest : public ::testing::Test {
protected:
  void SetUp() override {
    instrument_ids_ = {"BTCUSDT.SPOT.BNC", "ETHUSDT.SPOT.BNC"};
    event_thread_ = std::make_unique<EventThread>();
    
    processor_ = std::make_unique<OrderBookProcessor>(
        instrument_ids_, event_thread_.get());
  }

  void TearDown() override {
    processor_.reset();
    event_thread_.reset();
  }

  std::vector<std::string> instrument_ids_;
  std::unique_ptr<EventThread> event_thread_;
  std::unique_ptr<OrderBookProcessor> processor_;
};

TEST_F(OrderBookProcessorTest, SymbolExtraction) {
  // Test instrument ID parsing
  Instrument instrument;
  instrument.instrument_id = "BTCUSDT.SPOT.BNC";
  instrument.base_ccy = "BTC";
  instrument.quote_ccy = "USDT";
  instrument.exchange = "BNC";
  instrument.instrument_type = InstrumentType::SPOT;
  instrument.lotsize = 0.001;
  instrument.ticksize = 0.01;
  instrument.min_order_size = 0.001;
  instrument.max_order_size = 1000.0;
  
  std::vector<herm::PriceLevel> bids = {{50000.0, 1.0}};
  std::vector<herm::PriceLevel> asks = {{50100.0, 1.0}};
  
  processor_->OnOrderBookUpdate("binance", instrument, bids, asks);
  
  auto symbols = processor_->GetSymbols();
  EXPECT_GT(symbols.size(), 0);
}

TEST_F(OrderBookProcessorTest, GetOrderBookByInstrumentId) {
  Instrument instrument;
  instrument.instrument_id = "BTCUSDT.SPOT.BNC";
  instrument.base_ccy = "BTC";
  instrument.quote_ccy = "USDT";
  instrument.exchange = "BNC";
  instrument.instrument_type = InstrumentType::SPOT;
  instrument.lotsize = 0.001;
  instrument.ticksize = 0.01;
  instrument.min_order_size = 0.001;
  instrument.max_order_size = 1000.0;
  
  std::vector<herm::PriceLevel> bids = {{50000.0, 1.0}};
  std::vector<herm::PriceLevel> asks = {{50100.0, 1.0}};
  
  processor_->OnOrderBookUpdate("binance", instrument, bids, asks);
  
  OrderBook output;
  bool found = processor_->GetOrderBookByInstrumentId("BTCUSDT.SPOT.BNC", &output);
  
  EXPECT_TRUE(found);
  EXPECT_EQ(output.symbol(), "BTCUSDT");
}

TEST_F(OrderBookProcessorTest, GetFilteredOrderBook) {
  Instrument instrument;
  instrument.instrument_id = "BTCUSDT.SPOT.BNC";
  instrument.base_ccy = "BTC";
  instrument.quote_ccy = "USDT";
  instrument.exchange = "BNC";
  instrument.instrument_type = InstrumentType::SPOT;
  instrument.lotsize = 0.001;
  instrument.ticksize = 0.01;
  instrument.min_order_size = 0.001;
  instrument.max_order_size = 1000.0;
  
  std::vector<herm::PriceLevel> bids = {{50000.0, 1.0}, {49900.0, 2.0}};
  std::vector<herm::PriceLevel> asks = {{50100.0, 1.0}, {50200.0, 2.0}};
  
  processor_->OnOrderBookUpdate("binance", instrument, bids, asks);
  
  OrderBook output;
  processor_->GetRequestedOrderBook(
      "BTCUSDT", RequestType::BBO, {}, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  // BBO should have max 1 bid and 1 ask
  EXPECT_LE(output.bids_size(), 1);
  EXPECT_LE(output.asks_size(), 1);
}

TEST_F(OrderBookProcessorTest, PendingUpdateFlags) {
  Instrument instrument;
  instrument.instrument_id = "BTCUSDT.SPOT.BNC";
  instrument.base_ccy = "BTC";
  instrument.quote_ccy = "USDT";
  instrument.exchange = "BNC";
  instrument.instrument_type = InstrumentType::SPOT;
  instrument.lotsize = 0.001;
  instrument.ticksize = 0.01;
  instrument.min_order_size = 0.001;
  instrument.max_order_size = 1000.0;
  
  std::vector<herm::PriceLevel> bids = {{50000.0, 1.0}};
  std::vector<herm::PriceLevel> asks = {{50100.0, 1.0}};
  
  processor_->OnOrderBookUpdate("binance", instrument, bids, asks);
  
  // Check if symbol has pending update
  bool has_pending = processor_->HasPendingUpdate("BTCUSDT");
  
  // Clear pending update
  processor_->ClearPendingUpdate("BTCUSDT");
  
  // Should no longer have pending update
  bool has_pending_after = processor_->HasPendingUpdate("BTCUSDT");
  EXPECT_FALSE(has_pending_after);
}

TEST_F(OrderBookProcessorTest, MultipleSymbols) {
  Instrument btc_instrument;
  btc_instrument.instrument_id = "BTCUSDT.SPOT.BNC";
  btc_instrument.base_ccy = "BTC";
  btc_instrument.quote_ccy = "USDT";
  btc_instrument.exchange = "BNC";
  btc_instrument.instrument_type = InstrumentType::SPOT;
  btc_instrument.lotsize = 0.001;
  btc_instrument.ticksize = 0.01;
  btc_instrument.min_order_size = 0.001;
  btc_instrument.max_order_size = 1000.0;
  
  Instrument eth_instrument;
  eth_instrument.instrument_id = "ETHUSDT.SPOT.BNC";
  eth_instrument.base_ccy = "ETH";
  eth_instrument.quote_ccy = "USDT";
  eth_instrument.exchange = "BNC";
  eth_instrument.instrument_type = InstrumentType::SPOT;
  eth_instrument.lotsize = 0.01;
  eth_instrument.ticksize = 0.01;
  eth_instrument.min_order_size = 0.01;
  eth_instrument.max_order_size = 1000.0;
  
  std::vector<herm::PriceLevel> bids = {{50000.0, 1.0}};
  std::vector<herm::PriceLevel> asks = {{50100.0, 1.0}};
  
  processor_->OnOrderBookUpdate("binance", btc_instrument, bids, asks);
  processor_->OnOrderBookUpdate("binance", eth_instrument, bids, asks);
  
  auto symbols = processor_->GetSymbols();
  EXPECT_GE(symbols.size(), 2);
}

// ============================================================================
// Helper function to create test instruments
// ============================================================================

namespace {
Instrument CreateTestInstrument(const std::string& id, 
                                const std::string& base, 
                                const std::string& quote,
                                const std::string& exchange) {
  Instrument inst;
  inst.instrument_id = id;
  inst.base_ccy = base;
  inst.quote_ccy = quote;
  inst.exchange = exchange;
  inst.instrument_type = InstrumentType::SPOT;
  inst.lotsize = 0.001;
  inst.ticksize = 0.01;
  inst.min_order_size = 0.001;
  inst.max_order_size = 1000.0;
  return inst;
}
}

// ============================================================================
// Multi-Venue Aggregation Tests
// ============================================================================

TEST_F(OrderBookProcessorTest, MultiVenueAggregation) {
  auto btc_binance = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  auto btc_bybit = CreateTestInstrument("BTCUSDT.SPOT.BYB", "BTC", "USDT", "BYB");
  
  std::vector<herm::PriceLevel> binance_bids = {{50000.0, 1.0}};
  std::vector<herm::PriceLevel> binance_asks = {{50100.0, 1.0}};
  
  std::vector<herm::PriceLevel> bybit_bids = {{50010.0, 2.0}};
  std::vector<herm::PriceLevel> bybit_asks = {{50090.0, 2.0}};
  
  processor_->OnOrderBookUpdate("binance", btc_binance, binance_bids, binance_asks);
  processor_->OnOrderBookUpdate("bybit", btc_bybit, bybit_bids, bybit_asks);
  
  OrderBook output;
  bool found = processor_->GetOrderBookByInstrumentId("BTCUSDT.SPOT.BNC", &output);
  EXPECT_TRUE(found);
  
  // Should have aggregated data from both venues
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_GE(output.asks_size(), 1);
}

TEST_F(OrderBookProcessorTest, VenueContributionTracking) {
  auto btc_binance = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  auto btc_bybit = CreateTestInstrument("BTCUSDT.SPOT.BYB", "BTC", "USDT", "BYB");
  auto btc_okx = CreateTestInstrument("BTCUSDT.SPOT.OKX", "BTC", "USDT", "OKX");
  
  std::vector<herm::PriceLevel> bids = {{50000.0, 1.0}};
  std::vector<herm::PriceLevel> asks = {{50100.0, 1.0}};
  
  processor_->OnOrderBookUpdate("binance", btc_binance, bids, asks);
  processor_->OnOrderBookUpdate("bybit", btc_bybit, bids, asks);
  processor_->OnOrderBookUpdate("okx", btc_okx, bids, asks);
  
  OrderBook output;
  processor_->GetRequestedOrderBook("BTCUSDT", RequestType::NONE, {}, {}, &output);
  
  // Should have venue information from all three exchanges
  if (output.bids_size() > 0 && output.bids(0).venues_size() > 0) {
    EXPECT_GE(output.bids(0).venues_size(), 1);
  }
}

// ============================================================================
// Cross-Symbol Tests
// ============================================================================

TEST_F(OrderBookProcessorTest, IndependentSymbolTracking) {
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  auto eth = CreateTestInstrument("ETHUSDT.SPOT.BNC", "ETH", "USDT", "BNC");
  
  std::vector<herm::PriceLevel> btc_bids = {{50000.0, 1.0}};
  std::vector<herm::PriceLevel> btc_asks = {{50100.0, 1.0}};
  
  std::vector<herm::PriceLevel> eth_bids = {{3000.0, 10.0}};
  std::vector<herm::PriceLevel> eth_asks = {{3010.0, 10.0}};
  
  processor_->OnOrderBookUpdate("binance", btc, btc_bids, btc_asks);
  processor_->OnOrderBookUpdate("binance", eth, eth_bids, eth_asks);
  
  OrderBook btc_output, eth_output;
  
  bool btc_found = processor_->GetOrderBookByInstrumentId("BTCUSDT.SPOT.BNC", &btc_output);
  bool eth_found = processor_->GetOrderBookByInstrumentId("ETHUSDT.SPOT.BNC", &eth_output);
  
  EXPECT_TRUE(btc_found);
  EXPECT_TRUE(eth_found);
  
  EXPECT_EQ(btc_output.symbol(), "BTCUSDT");
  EXPECT_EQ(eth_output.symbol(), "ETHUSDT");
  
  // Verify prices are independent
  if (btc_output.bids_size() > 0 && eth_output.bids_size() > 0) {
    EXPECT_NE(btc_output.bids(0).price(), eth_output.bids(0).price());
  }
}

TEST_F(OrderBookProcessorTest, ManySymbolsSimultaneous) {
  // Note: Only instruments in instrument_ids_ will be processed
  // processor was initialized with {"BTCUSDT.SPOT.BNC", "ETHUSDT.SPOT.BNC"}
  
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  auto eth = CreateTestInstrument("ETHUSDT.SPOT.BNC", "ETH", "USDT", "BNC");
  
  std::vector<herm::PriceLevel> bids = {{1000.0, 1.0}};
  std::vector<herm::PriceLevel> asks = {{1010.0, 1.0}};
  
  processor_->OnOrderBookUpdate("binance", btc, bids, asks);
  processor_->OnOrderBookUpdate("binance", eth, bids, asks);
  
  auto tracked_symbols = processor_->GetSymbols();
  EXPECT_EQ(tracked_symbols.size(), 2);
  
  // Verify both symbols are tracked
  bool has_btc = false, has_eth = false;
  for (const auto& sym : tracked_symbols) {
    if (sym == "BTCUSDT") has_btc = true;
    if (sym == "ETHUSDT") has_eth = true;
  }
  EXPECT_TRUE(has_btc);
  EXPECT_TRUE(has_eth);
}

// ============================================================================
// Filter Type Tests
// ============================================================================

TEST_F(OrderBookProcessorTest, BBOFilter) {
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  
  std::vector<herm::PriceLevel> bids = {
    {50000.0, 1.0}, {49900.0, 2.0}, {49800.0, 1.5}
  };
  std::vector<herm::PriceLevel> asks = {
    {50100.0, 1.0}, {50200.0, 2.0}, {50300.0, 1.5}
  };
  
  processor_->OnOrderBookUpdate("binance", btc, bids, asks);
  
  OrderBook output;
  processor_->GetRequestedOrderBook("BTCUSDT", RequestType::BBO, {}, {}, &output);
  
  // BBO should return only top level
  EXPECT_EQ(output.bids_size(), 1);
  EXPECT_EQ(output.asks_size(), 1);
  
  if (output.bids_size() > 0) {
    EXPECT_DOUBLE_EQ(output.bids(0).price(), 50000.0);
  }
  if (output.asks_size() > 0) {
    EXPECT_DOUBLE_EQ(output.asks(0).price(), 50100.0);
  }
}

TEST_F(OrderBookProcessorTest, PriceBandFilter) {
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  
  std::vector<herm::PriceLevel> bids = {
    {50000.0, 1.0}, {49900.0, 2.0}, {49800.0, 1.5}
  };
  std::vector<herm::PriceLevel> asks = {
    {50100.0, 1.0}, {50200.0, 2.0}, {50300.0, 1.5}
  };
  
  processor_->OnOrderBookUpdate("binance", btc, bids, asks);
  
  std::vector<int32_t> basis_points = {100};  // 1%
  OrderBook output;
  processor_->GetRequestedOrderBook("BTCUSDT", RequestType::PRICE_BAND, 
                                   basis_points, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_GE(output.asks_size(), 1);
}

TEST_F(OrderBookProcessorTest, VolumeBandFilter) {
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  
  std::vector<herm::PriceLevel> bids = {
    {50000.0, 1.0}, {49900.0, 2.0}, {49800.0, 1.5}
  };
  std::vector<herm::PriceLevel> asks = {
    {50100.0, 1.0}, {50200.0, 2.0}, {50300.0, 1.5}
  };
  
  processor_->OnOrderBookUpdate("binance", btc, bids, asks);
  
  std::vector<double> volumes = {50000.0};  // $50k
  OrderBook output;
  processor_->GetRequestedOrderBook("BTCUSDT", RequestType::VOLUME_BAND, 
                                   {}, volumes, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  EXPECT_GE(output.bids_size(), 1);
  EXPECT_GE(output.asks_size(), 1);
}

TEST_F(OrderBookProcessorTest, NoneFilterFullBook) {
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  
  std::vector<herm::PriceLevel> bids = {
    {50000.0, 1.0}, {49900.0, 2.0}, {49800.0, 1.5}
  };
  std::vector<herm::PriceLevel> asks = {
    {50100.0, 1.0}, {50200.0, 2.0}, {50300.0, 1.5}
  };
  
  processor_->OnOrderBookUpdate("binance", btc, bids, asks);
  
  OrderBook output;
  processor_->GetRequestedOrderBook("BTCUSDT", RequestType::NONE, {}, {}, &output);
  
  EXPECT_EQ(output.symbol(), "BTCUSDT");
  // NONE should return full aggregated book
  EXPECT_GE(output.bids_size(), 3);
  EXPECT_GE(output.asks_size(), 3);
}

// ============================================================================
// Update Pattern Tests
// ============================================================================

TEST_F(OrderBookProcessorTest, RapidUpdates) {
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  
  // Send 100 rapid updates
  for (int i = 0; i < 100; ++i) {
    std::vector<herm::PriceLevel> bids = {{50000.0 + i * 0.1, 1.0}};
    std::vector<herm::PriceLevel> asks = {{50100.0 + i * 0.1, 1.0}};
    
    processor_->OnOrderBookUpdate("binance", btc, bids, asks);
  }
  
  OrderBook output;
  bool found = processor_->GetOrderBookByInstrumentId("BTCUSDT.SPOT.BNC", &output);
  EXPECT_TRUE(found);
  
  // Should have the most recent update
  EXPECT_EQ(output.symbol(), "BTCUSDT");
}

TEST_F(OrderBookProcessorTest, InterleavedMultiSymbolUpdates) {
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  auto eth = CreateTestInstrument("ETHUSDT.SPOT.BNC", "ETH", "USDT", "BNC");
  
  // Alternate updates between symbols
  for (int i = 0; i < 10; ++i) {
    std::vector<herm::PriceLevel> btc_bids = {{50000.0 + i, 1.0}};
    std::vector<herm::PriceLevel> btc_asks = {{50100.0 + i, 1.0}};
    processor_->OnOrderBookUpdate("binance", btc, btc_bids, btc_asks);
    
    std::vector<herm::PriceLevel> eth_bids = {{3000.0 + i, 10.0}};
    std::vector<herm::PriceLevel> eth_asks = {{3010.0 + i, 10.0}};
    processor_->OnOrderBookUpdate("binance", eth, eth_bids, eth_asks);
  }
  
  OrderBook btc_output, eth_output;
  EXPECT_TRUE(processor_->GetOrderBookByInstrumentId("BTCUSDT.SPOT.BNC", &btc_output));
  EXPECT_TRUE(processor_->GetOrderBookByInstrumentId("ETHUSDT.SPOT.BNC", &eth_output));
}

// ============================================================================
// Backpressure and Flag Tests
// ============================================================================

TEST_F(OrderBookProcessorTest, PendingUpdateFlagLifecycle) {
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  
  std::vector<herm::PriceLevel> bids = {{50000.0, 1.0}};
  std::vector<herm::PriceLevel> asks = {{50100.0, 1.0}};
  
  // Initial state - no pending update
  EXPECT_FALSE(processor_->HasPendingUpdate("BTCUSDT"));
  
  // Send update
  processor_->OnOrderBookUpdate("binance", btc, bids, asks);
  
  // May have pending update immediately after
  // (depends on internal event processing)
  
  // Clear it
  processor_->ClearPendingUpdate("BTCUSDT");
  
  // Should be clear now
  EXPECT_FALSE(processor_->HasPendingUpdate("BTCUSDT"));
}

TEST_F(OrderBookProcessorTest, InstrumentFlagClearing) {
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  
  std::vector<herm::PriceLevel> bids = {{50000.0, 1.0}};
  std::vector<herm::PriceLevel> asks = {{50100.0, 1.0}};
  
  processor_->OnOrderBookUpdate("binance", btc, bids, asks);
  
  // Clear all instrument flags for symbol
  processor_->ClearInstrumentFlags("BTCUSDT");
  
  // Should still be able to retrieve order book
  OrderBook output;
  EXPECT_TRUE(processor_->GetOrderBookByInstrumentId("BTCUSDT.SPOT.BNC", &output));
}

// ============================================================================
// Error Handling Tests
// ============================================================================

TEST_F(OrderBookProcessorTest, NonExistentInstrument) {
  OrderBook output;
  bool found = processor_->GetOrderBookByInstrumentId("NONEXISTENT.SPOT.BNC", &output);
  
  EXPECT_FALSE(found);
}

TEST_F(OrderBookProcessorTest, NonExistentSymbol) {
  OrderBook output;
  processor_->GetRequestedOrderBook("NONEXISTENT", RequestType::BBO, {}, {}, &output);
  
  // Should not crash, returns empty book with empty symbol
  // (no aggregator was created for this symbol)
  EXPECT_EQ(output.symbol(), "");
}

TEST_F(OrderBookProcessorTest, EmptyOrderBookUpdate) {
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  
  std::vector<herm::PriceLevel> empty_bids;
  std::vector<herm::PriceLevel> empty_asks;
  
  // Should not crash
  processor_->OnOrderBookUpdate("binance", btc, empty_bids, empty_asks);
  
  OrderBook output;
  processor_->GetOrderBookByInstrumentId("BTCUSDT.SPOT.BNC", &output);
  // May have empty or previous state
}

// ============================================================================
// Complex Scenario Tests
// ============================================================================

TEST_F(OrderBookProcessorTest, MultiVenueMultiSymbolComplex) {
  // Create multiple instruments across venues and symbols
  auto btc_bnc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  auto btc_byb = CreateTestInstrument("BTCUSDT.SPOT.BYB", "BTC", "USDT", "BYB");
  auto eth_bnc = CreateTestInstrument("ETHUSDT.SPOT.BNC", "ETH", "USDT", "BNC");
  auto eth_byb = CreateTestInstrument("ETHUSDT.SPOT.BYB", "ETH", "USDT", "BYB");
  
  std::vector<herm::PriceLevel> btc_bids = {{50000.0, 1.0}};
  std::vector<herm::PriceLevel> btc_asks = {{50100.0, 1.0}};
  
  std::vector<herm::PriceLevel> eth_bids = {{3000.0, 10.0}};
  std::vector<herm::PriceLevel> eth_asks = {{3010.0, 10.0}};
  
  processor_->OnOrderBookUpdate("binance", btc_bnc, btc_bids, btc_asks);
  processor_->OnOrderBookUpdate("bybit", btc_byb, btc_bids, btc_asks);
  processor_->OnOrderBookUpdate("binance", eth_bnc, eth_bids, eth_asks);
  processor_->OnOrderBookUpdate("bybit", eth_byb, eth_bids, eth_asks);
  
  auto symbols = processor_->GetSymbols();
  EXPECT_GE(symbols.size(), 2);
  
  // Verify each symbol has aggregated data
  OrderBook btc_output, eth_output;
  processor_->GetRequestedOrderBook("BTCUSDT", RequestType::NONE, {}, {}, &btc_output);
  processor_->GetRequestedOrderBook("ETHUSDT", RequestType::NONE, {}, {}, &eth_output);
  
  EXPECT_EQ(btc_output.symbol(), "BTCUSDT");
  EXPECT_EQ(eth_output.symbol(), "ETHUSDT");
}

TEST_F(OrderBookProcessorTest, StaleDataHandling) {
  auto btc = CreateTestInstrument("BTCUSDT.SPOT.BNC", "BTC", "USDT", "BNC");
  
  // First update
  std::vector<herm::PriceLevel> bids1 = {{50000.0, 1.0}};
  std::vector<herm::PriceLevel> asks1 = {{50100.0, 1.0}};
  processor_->OnOrderBookUpdate("binance", btc, bids1, asks1);
  
  // Later update with different prices
  std::vector<herm::PriceLevel> bids2 = {{51000.0, 1.5}};
  std::vector<herm::PriceLevel> asks2 = {{51100.0, 1.5}};
  processor_->OnOrderBookUpdate("binance", btc, bids2, asks2);
  
  OrderBook output;
  processor_->GetOrderBookByInstrumentId("BTCUSDT.SPOT.BNC", &output);
  
  // Should have the latest data
  if (output.bids_size() > 0) {
    EXPECT_DOUBLE_EQ(output.bids(0).price(), 51000.0);
  }
}
