#pragma once

#include <string>
#include <vector>
#include "engine/market_data/order_book.hpp"

namespace herm {

/**
 * @brief Base class for exchange HTTP clients
 * 
 * Provides common SSL/HTTP functionality for fetching order book snapshots.
 * Subclasses implement exchange-specific URL construction and JSON parsing.
 */
class BaseHttpClient {
 public:
  BaseHttpClient() = default;
  virtual ~BaseHttpClient() = default;

  // Non-copyable, movable
  BaseHttpClient(const BaseHttpClient&) = delete;
  BaseHttpClient& operator=(const BaseHttpClient&) = delete;
  BaseHttpClient(BaseHttpClient&&) = default;
  BaseHttpClient& operator=(BaseHttpClient&&) = default;

  /**
   * @brief Depth snapshot structure used by all exchanges
   */
  struct DepthSnapshot {
    int64_t lastUpdateId = 0;
    std::vector<PriceLevel> bids;
    std::vector<PriceLevel> asks;
  };

  /**
   * @brief Set the base URL for REST API requests
   * 
   * Allows overriding the default production URL (e.g., for testing with mocks).
   * Format: "https://hostname" or "https://hostname:port"
   * 
   * @param base_url Base URL without path (e.g., "https://api.binance.com" or "https://localhost:8091")
   */
  virtual void SetBaseUrl(const std::string& base_url) {
    base_url_ = base_url;
  }

 protected:
  /**
   * @brief Fetch depth snapshot via HTTPS GET request
   * 
   * Template method that handles:
   * - SSL context creation
   * - Host resolution
   * - Connection establishment
   * - HTTP GET request
   * - Response handling
   * - Graceful shutdown
   * 
   * @param host Exchange hostname (e.g., "api.binance.com")
   * @param path API endpoint path (e.g., "/api/v3/depth?symbol=BTCUSDT&limit=5000")
   * @param exchange_name Exchange name for logging
   * @return DepthSnapshot (empty on error)
   */
  DepthSnapshot FetchDepthSnapshot(const std::string& host,
                                    const std::string& path,
                                    const std::string& exchange_name);

  /**
   * @brief Parse exchange-specific JSON response
   * 
   * Virtual method implemented by subclasses to parse their specific
   * JSON response format into the common DepthSnapshot structure.
   * 
   * @param json_response Raw JSON response body
   * @return Parsed DepthSnapshot
   */
  virtual DepthSnapshot ParseDepthResponse(const std::string& json_response) = 0;

  std::string base_url_;  ///< Configurable base URL (empty = use default)
};

}  // namespace herm
