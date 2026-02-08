#pragma once

#include "market_data.grpc.pb.h"
#include <nlohmann/json.hpp>
#include <string>

namespace herm {
namespace engine {
namespace publishing {

/**
 * @brief Converts protobuf OrderBook to JSON
 * 
 * Produces JSON matching protobuf structure exactly:
 * {
 *   "symbol": "BTCUSDT",
 *   "timestamp_us": 1234567890,
 *   "bids": [{price, quantity, venues}],
 *   "asks": [{price, quantity, venues}],
 *   "venue_timestamps": {"venue1": timestamp, ...}
 * }
 * 
 * All methods are static (stateless converter).
 */
class OrderBookJsonConverter {
 public:
  /** @brief Convert to JSON string */
  static std::string ToJson(const herm::market_data::OrderBook& order_book);
  
  /** @brief Convert to JSON object */
  static nlohmann::json ToJsonObject(const herm::market_data::OrderBook& order_book);
  
 private:
  /** @brief Convert single price level */
  static nlohmann::json PriceLevelToJson(const herm::market_data::PriceLevel& level);
  
  /** @brief Convert venue timestamp map */
  static nlohmann::json VenueTimestampsToJson(
      const google::protobuf::Map<std::string, int64_t>& timestamps);
};

}  // namespace publishing
}  // namespace engine
}  // namespace herm
