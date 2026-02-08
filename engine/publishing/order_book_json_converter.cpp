#include "order_book_json_converter.hpp"
#include <sstream>
#include <iomanip>

namespace herm {
namespace engine {
namespace publishing {

std::string OrderBookJsonConverter::ToJson(const herm::market_data::OrderBook& order_book) {
  nlohmann::json json_obj = ToJsonObject(order_book);
  return json_obj.dump();
}

nlohmann::json OrderBookJsonConverter::ToJsonObject(const herm::market_data::OrderBook& order_book) {
  nlohmann::json json_obj;
  
  // Set basic fields matching proto structure
  json_obj["symbol"] = order_book.symbol();
  json_obj["timestamp_us"] = order_book.timestamp_us();
  
  // Convert bids array
  nlohmann::json bids_array = nlohmann::json::array();
  for (int i = 0; i < order_book.bids_size(); ++i) {
    bids_array.push_back(PriceLevelToJson(order_book.bids(i)));
  }
  json_obj["bids"] = bids_array;
  
  // Convert asks array
  nlohmann::json asks_array = nlohmann::json::array();
  for (int i = 0; i < order_book.asks_size(); ++i) {
    asks_array.push_back(PriceLevelToJson(order_book.asks(i)));
  }
  json_obj["asks"] = asks_array;
  
  // Convert venue timestamps map
  if (order_book.venue_timestamps_size() > 0) {
    json_obj["venue_timestamps"] = VenueTimestampsToJson(order_book.venue_timestamps());
  }
  
  return json_obj;
}

nlohmann::json OrderBookJsonConverter::PriceLevelToJson(const herm::market_data::PriceLevel& level) {
  nlohmann::json level_json;
  level_json["price"] = level.price();
  level_json["quantity"] = level.quantity();
  
  // Convert venues array if present
  if (level.venues_size() > 0) {
    nlohmann::json venues_array = nlohmann::json::array();
    for (int i = 0; i < level.venues_size(); ++i) {
      nlohmann::json venue_json;
      venue_json["venue_name"] = level.venues(i).venue_name();
      venue_json["quantity"] = level.venues(i).quantity();
      venues_array.push_back(venue_json);
    }
    level_json["venues"] = venues_array;
  }
  
  return level_json;
}

nlohmann::json OrderBookJsonConverter::VenueTimestampsToJson(
    const google::protobuf::Map<std::string, int64_t>& timestamps) {
  nlohmann::json timestamps_obj;
  for (const auto& [venue_name, timestamp] : timestamps) {
    timestamps_obj[venue_name] = timestamp;
  }
  return timestamps_obj;
}

}  // namespace publishing
}  // namespace engine
}  // namespace herm
