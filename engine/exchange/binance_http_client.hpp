#pragma once

#include "base_http_client.hpp"
#include <string>

namespace herm {

/**
 * @brief HTTP client for Binance REST API calls
 * 
 * Supports both Spot and Futures APIs with different hosts and endpoints.
 */
class BinanceHttpClient : public BaseHttpClient {
 public:
  BinanceHttpClient() = default;
  ~BinanceHttpClient() override = default;

  /**
   * @brief Get depth snapshot from Binance REST API
   * 
   * @param symbol e.g., "BTCUSDT" (uppercase for both spot and futures)
   * @param is_futures true for futures API, false for spot API
   * @return DepthSnapshot (empty on error)
   */
  DepthSnapshot GetDepthSnapshot(const std::string& symbol, bool is_futures);

 protected:
  /**
   * @brief Parse Binance-specific JSON response
   * 
   * Binance format:
   * {
   *   "lastUpdateId": 1234567890,
   *   "bids": [["99000.00", "1.5"], ...],
   *   "asks": [["99100.00", "2.3"], ...]
   * }
   */
  DepthSnapshot ParseDepthResponse(const std::string& json_response) override;
};

}  // namespace herm
