#pragma once

#include "base_http_client.hpp"
#include <string>

namespace herm {

/**
 * @brief HTTP client for OKX REST API calls
 * 
 * Uses V5 API with same endpoint for spot and perpetual.
 */
class OKXHttpClient : public BaseHttpClient {
 public:
  OKXHttpClient() = default;
  ~OKXHttpClient() override = default;

  /**
   * @brief Get depth snapshot from OKX REST API
   * 
   * @param symbol e.g., "BTC-USDT" (spot) or "BTC-USDT-SWAP" (perp)
   * @param is_spot true for spot API, false for perpetual API (used for logging)
   * @return DepthSnapshot (empty on error)
   */
  DepthSnapshot GetDepthSnapshot(const std::string& symbol, bool is_spot);

 protected:
  /**
   * @brief Parse OKX-specific JSON response
   * 
   * OKX format:
   * {
   *   "code": "0",
   *   "data": [{
   *     "asks": [["99100.00", "2.3", "0", "1"], ...],
   *     "bids": [["99000.00", "1.5", "0", "1"], ...],
   *     "ts": "1234567890"
   *   }]
   * }
   */
  DepthSnapshot ParseDepthResponse(const std::string& json_response) override;
};

}  // namespace herm
