#include <gtest/gtest.h>
#include "publishing/order_book_json_converter.hpp"
#include "market_data.grpc.pb.h"
#include <nlohmann/json.hpp>

using namespace herm::engine::publishing;
using namespace herm::market_data;

class OrderBookJsonConverterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a test order book
    order_book_.set_symbol("BTCUSDT");
    order_book_.set_timestamp_us(1234567890);
    
    // Add bid levels
    auto* bid1 = order_book_.add_bids();
    bid1->set_price(50000.0);
    bid1->set_quantity(1.5);
    auto* venue1 = bid1->add_venues();
    venue1->set_venue_name("binance");
    venue1->set_quantity(1.0);
    auto* venue2 = bid1->add_venues();
    venue2->set_venue_name("bybit");
    venue2->set_quantity(0.5);
    
    auto* bid2 = order_book_.add_bids();
    bid2->set_price(49900.0);
    bid2->set_quantity(2.0);
    
    // Add ask levels
    auto* ask1 = order_book_.add_asks();
    ask1->set_price(50100.0);
    ask1->set_quantity(1.2);
    auto* venue3 = ask1->add_venues();
    venue3->set_venue_name("binance");
    venue3->set_quantity(1.2);
    
    auto* ask2 = order_book_.add_asks();
    ask2->set_price(50200.0);
    ask2->set_quantity(3.0);
    
    // Add venue timestamps
    (*order_book_.mutable_venue_timestamps())["binance"] = 1234567890;
    (*order_book_.mutable_venue_timestamps())["bybit"] = 1234567891;
  }

  OrderBook order_book_;
};

TEST_F(OrderBookJsonConverterTest, BasicConversion) {
  std::string json_str = OrderBookJsonConverter::ToJson(order_book_);
  ASSERT_FALSE(json_str.empty());
  
  nlohmann::json json_obj = nlohmann::json::parse(json_str);
  
  EXPECT_EQ(json_obj["symbol"], "BTCUSDT");
  EXPECT_EQ(json_obj["timestamp_us"], 1234567890);
  EXPECT_TRUE(json_obj.contains("bids"));
  EXPECT_TRUE(json_obj.contains("asks"));
  EXPECT_TRUE(json_obj.contains("venue_timestamps"));
}

TEST_F(OrderBookJsonConverterTest, BidsStructure) {
  nlohmann::json json_obj = OrderBookJsonConverter::ToJsonObject(order_book_);
  
  ASSERT_TRUE(json_obj.contains("bids"));
  ASSERT_TRUE(json_obj["bids"].is_array());
  EXPECT_EQ(json_obj["bids"].size(), 2);
  
  // Check first bid
  auto bid1 = json_obj["bids"][0];
  EXPECT_DOUBLE_EQ(bid1["price"], 50000.0);
  EXPECT_DOUBLE_EQ(bid1["quantity"], 1.5);
  ASSERT_TRUE(bid1.contains("venues"));
  ASSERT_TRUE(bid1["venues"].is_array());
  EXPECT_EQ(bid1["venues"].size(), 2);
  EXPECT_EQ(bid1["venues"][0]["venue_name"], "binance");
  EXPECT_DOUBLE_EQ(bid1["venues"][0]["quantity"], 1.0);
  EXPECT_EQ(bid1["venues"][1]["venue_name"], "bybit");
  EXPECT_DOUBLE_EQ(bid1["venues"][1]["quantity"], 0.5);
  
  // Check second bid
  auto bid2 = json_obj["bids"][1];
  EXPECT_DOUBLE_EQ(bid2["price"], 49900.0);
  EXPECT_DOUBLE_EQ(bid2["quantity"], 2.0);
  EXPECT_FALSE(bid2.contains("venues"));
}

TEST_F(OrderBookJsonConverterTest, AsksStructure) {
  nlohmann::json json_obj = OrderBookJsonConverter::ToJsonObject(order_book_);
  
  ASSERT_TRUE(json_obj.contains("asks"));
  ASSERT_TRUE(json_obj["asks"].is_array());
  EXPECT_EQ(json_obj["asks"].size(), 2);
  
  // Check first ask
  auto ask1 = json_obj["asks"][0];
  EXPECT_DOUBLE_EQ(ask1["price"], 50100.0);
  EXPECT_DOUBLE_EQ(ask1["quantity"], 1.2);
  ASSERT_TRUE(ask1.contains("venues"));
  ASSERT_TRUE(ask1["venues"].is_array());
  EXPECT_EQ(ask1["venues"].size(), 1);
  EXPECT_EQ(ask1["venues"][0]["venue_name"], "binance");
  EXPECT_DOUBLE_EQ(ask1["venues"][0]["quantity"], 1.2);
  
  // Check second ask
  auto ask2 = json_obj["asks"][1];
  EXPECT_DOUBLE_EQ(ask2["price"], 50200.0);
  EXPECT_DOUBLE_EQ(ask2["quantity"], 3.0);
}

TEST_F(OrderBookJsonConverterTest, VenueTimestamps) {
  nlohmann::json json_obj = OrderBookJsonConverter::ToJsonObject(order_book_);
  
  ASSERT_TRUE(json_obj.contains("venue_timestamps"));
  ASSERT_TRUE(json_obj["venue_timestamps"].is_object());
  EXPECT_EQ(json_obj["venue_timestamps"]["binance"], 1234567890);
  EXPECT_EQ(json_obj["venue_timestamps"]["bybit"], 1234567891);
}

TEST_F(OrderBookJsonConverterTest, EmptyOrderBook) {
  OrderBook empty_book;
  empty_book.set_symbol("ETHUSDT");
  empty_book.set_timestamp_us(0);
  
  nlohmann::json json_obj = OrderBookJsonConverter::ToJsonObject(empty_book);
  
  EXPECT_EQ(json_obj["symbol"], "ETHUSDT");
  EXPECT_EQ(json_obj["timestamp_us"], 0);
  ASSERT_TRUE(json_obj.contains("bids"));
  EXPECT_TRUE(json_obj["bids"].is_array());
  EXPECT_EQ(json_obj["bids"].size(), 0);
  ASSERT_TRUE(json_obj.contains("asks"));
  EXPECT_TRUE(json_obj["asks"].is_array());
  EXPECT_EQ(json_obj["asks"].size(), 0);
}

TEST_F(OrderBookJsonConverterTest, PriceLevelWithoutVenues) {
  OrderBook book;
  book.set_symbol("BTCUSDT");
  book.set_timestamp_us(1234567890);
  
  auto* bid = book.add_bids();
  bid->set_price(50000.0);
  bid->set_quantity(1.0);
  // No venues added
  
  nlohmann::json json_obj = OrderBookJsonConverter::ToJsonObject(book);
  auto bid_json = json_obj["bids"][0];
  
  EXPECT_DOUBLE_EQ(bid_json["price"], 50000.0);
  EXPECT_DOUBLE_EQ(bid_json["quantity"], 1.0);
  // Venues should not be present if empty
  EXPECT_FALSE(bid_json.contains("venues"));
}

TEST_F(OrderBookJsonConverterTest, MultipleVenuesPerLevel) {
  OrderBook book;
  book.set_symbol("BTCUSDT");
  book.set_timestamp_us(1234567890);
  
  auto* bid = book.add_bids();
  bid->set_price(50000.0);
  bid->set_quantity(5.0);
  
  // Add multiple venues
  auto* v1 = bid->add_venues();
  v1->set_venue_name("binance");
  v1->set_quantity(2.0);
  auto* v2 = bid->add_venues();
  v2->set_venue_name("bybit");
  v2->set_quantity(2.0);
  auto* v3 = bid->add_venues();
  v3->set_venue_name("okx");
  v3->set_quantity(1.0);
  
  nlohmann::json json_obj = OrderBookJsonConverter::ToJsonObject(book);
  auto venues = json_obj["bids"][0]["venues"];
  
  ASSERT_EQ(venues.size(), 3);
  EXPECT_EQ(venues[0]["venue_name"], "binance");
  EXPECT_DOUBLE_EQ(venues[0]["quantity"], 2.0);
  EXPECT_EQ(venues[1]["venue_name"], "bybit");
  EXPECT_DOUBLE_EQ(venues[1]["quantity"], 2.0);
  EXPECT_EQ(venues[2]["venue_name"], "okx");
  EXPECT_DOUBLE_EQ(venues[2]["quantity"], 1.0);
}

TEST_F(OrderBookJsonConverterTest, JsonStringIsValid) {
  std::string json_str = OrderBookJsonConverter::ToJson(order_book_);
  
  // Should be valid JSON
  nlohmann::json parsed = nlohmann::json::parse(json_str);
  EXPECT_EQ(parsed["symbol"], "BTCUSDT");
}

TEST_F(OrderBookJsonConverterTest, ProtoStructureMatch) {
  // Verify JSON structure exactly matches proto structure
  nlohmann::json json_obj = OrderBookJsonConverter::ToJsonObject(order_book_);
  
  // Required fields from proto
  EXPECT_TRUE(json_obj.contains("symbol"));
  EXPECT_TRUE(json_obj.contains("timestamp_us"));
  EXPECT_TRUE(json_obj.contains("bids"));
  EXPECT_TRUE(json_obj.contains("asks"));
  
  // Bids and asks must be arrays
  EXPECT_TRUE(json_obj["bids"].is_array());
  EXPECT_TRUE(json_obj["asks"].is_array());
  
  // Each price level should have price and quantity
  if (json_obj["bids"].size() > 0) {
    EXPECT_TRUE(json_obj["bids"][0].contains("price"));
    EXPECT_TRUE(json_obj["bids"][0].contains("quantity"));
  }
  
  if (json_obj["asks"].size() > 0) {
    EXPECT_TRUE(json_obj["asks"][0].contains("price"));
    EXPECT_TRUE(json_obj["asks"][0].contains("quantity"));
  }
}
