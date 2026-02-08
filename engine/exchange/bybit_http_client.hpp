#pragma once

#include "base_http_client.hpp"
#include <string>

namespace herm {

/**
 * @brief HTTP client for Bybit REST API calls
 * 
 * Uses V5 API with different category parameter for spot vs linear.
 */
class BybitHttpClient : public BaseHttpClient {
 public:
  BybitHttpClient() = default;
  ~BybitHttpClient() override = default;

  /**
   * @brief Get depth snapshot from Bybit REST API
   * 
   * @param symbol e.g., "BTCUSDT" (uppercase)
   * @param is_spot true for spot API, false for perpetual/linear API
   * @return DepthSnapshot (empty on error)
   */
  DepthSnapshot GetDepthSnapshot(const std::string& symbol, bool is_spot);

 protected:
  /**
   * @brief Parse Bybit-specific JSON response
   * 
   * Bybit format:
   * {
   *   "retCode": 0,
   *   "result": {
   *     "s": "BTCUSDT",
   *     "b": [["99000.00", "1.5"], ...],
   *     "a": [["99100.00", "2.3"], ...],
   *     "u": 1234567890
   *   }
   * }
   */
  DepthSnapshot ParseDepthResponse(const std::string& json_response) override;
};

}  // namespace herm
