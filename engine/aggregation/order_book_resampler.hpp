#pragma once

#include "engine/market_data/order_book_aggregator.hpp"
#include "market_data.grpc.pb.h"
#include <vector>

namespace herm {
namespace market_data {

/**
 * @brief Resamples and filters order book data
 * 
 * Provides static methods to transform full order books into filtered views:
 * - BBO: Best bid/offer only
 * - PRICE_BAND: Liquidity within basis point ranges
 * - VOLUME_BAND: VWAP for target notional volumes
 * 
 * All methods are thread-safe (stateless operations).
 */
class OrderBookResampler {
 public:
  /**
   * @brief Resample order book based on request type
   * @param full_book Source order book
   * @param request_type Filter type (BBO, PRICE_BAND, VOLUME_BAND, NONE)
   * @param basis_points Basis point levels (for PRICE_BAND)
   * @param notional_volumes Target volumes (for VOLUME_BAND)
   * @param output Filtered order book result
   */
  static void ResampleOrderBook(
      const ::herm::market_data::OrderBook& full_book,
      RequestType request_type,
      const std::vector<int32_t>& basis_points,
      const std::vector<double>& notional_volumes,
      ::herm::market_data::OrderBook* output);

 private:
  /** @brief Extract best bid/offer only */
  static void ApplyBBO(
      const ::herm::market_data::OrderBook& full_book,
      ::herm::market_data::OrderBook* output);
      
  /** @brief Aggregate liquidity at price band levels */
  static void ApplyPriceBand(
      const ::herm::market_data::OrderBook& full_book,
      const std::vector<int32_t>& basis_points,
      ::herm::market_data::OrderBook* output);
      
  /** @brief Calculate VWAP for target volumes */
  static void ApplyVolumeBand(
      const ::herm::market_data::OrderBook& full_book,
      const std::vector<double>& notional_volumes,
      ::herm::market_data::OrderBook* output);
};

}  // namespace market_data
}  // namespace herm
