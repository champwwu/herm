#include "binance_http_client.hpp"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>

namespace herm {

BaseHttpClient::DepthSnapshot BinanceHttpClient::GetDepthSnapshot(
    const std::string& symbol, bool is_futures) {
  // Determine host and path based on API type
  std::string host, path;
  
  // Check if custom base URL is set (for mocks/testing)
  if (!base_url_.empty()) {
    // Extract host from base_url (remove https:// prefix and optional port)
    host = base_url_;
    if (host.find("https://") == 0) {
      host = host.substr(8);  // Remove "https://"
    } else if (host.find("http://") == 0) {
      host = host.substr(7);  // Remove "http://"
    }
    // Path matches the production API format
    if (is_futures) {
      path = "/fapi/v1/depth?symbol=" + symbol + "&limit=1000";
    } else {
      path = "/api/v3/depth?symbol=" + symbol + "&limit=5000";
    }
  } else {
    // Use production URLs
    if (is_futures) {
      host = "fapi.binance.com";
      // Binance Futures API supports limits: 5, 10, 20, 50, 100, 500, 1000 (max)
      path = "/fapi/v1/depth?symbol=" + symbol + "&limit=1000";
    } else {
      host = "api.binance.com";
      // Binance Spot API supports limits up to 5000
      path = "/api/v3/depth?symbol=" + symbol + "&limit=5000";
    }
  }

  return FetchDepthSnapshot(host, path, "BinanceHttpClient");
}

BaseHttpClient::DepthSnapshot BinanceHttpClient::ParseDepthResponse(
    const std::string& json_response) {
  DepthSnapshot snapshot;

  try {
    auto json = nlohmann::json::parse(json_response);

    // Extract lastUpdateId
    if (json.contains("lastUpdateId") && json["lastUpdateId"].is_number()) {
      snapshot.lastUpdateId = json["lastUpdateId"].get<int64_t>();
    }

    // Parse bids
    if (json.contains("bids") && json["bids"].is_array()) {
      for (const auto& bid : json["bids"]) {
        if (bid.is_array() && bid.size() >= 2) {
          std::string price_str = bid[0].get<std::string>();
          std::string qty_str = bid[1].get<std::string>();
          double price = std::stod(price_str);
          double qty = std::stod(qty_str);
          if (qty > 0.0) {
            snapshot.bids.emplace_back(price, qty);
          }
        }
      }
    }

    // Parse asks
    if (json.contains("asks") && json["asks"].is_array()) {
      for (const auto& ask : json["asks"]) {
        if (ask.is_array() && ask.size() >= 2) {
          std::string price_str = ask[0].get<std::string>();
          std::string qty_str = ask[1].get<std::string>();
          double price = std::stod(price_str);
          double qty = std::stod(qty_str);
          if (qty > 0.0) {
            snapshot.asks.emplace_back(price, qty);
          }
        }
      }
    }

    // Sort bids descending, asks ascending (Binance should already be sorted, but ensure)
    std::sort(snapshot.bids.begin(), snapshot.bids.end(),
              [](const PriceLevel& a, const PriceLevel& b) { return a.price > b.price; });
    std::sort(snapshot.asks.begin(), snapshot.asks.end(),
              [](const PriceLevel& a, const PriceLevel& b) { return a.price < b.price; });

  } catch (const nlohmann::json::exception& e) {
    SPDLOG_ERROR("BinanceHttpClient: JSON parse error: {}", e.what());
  } catch (const std::exception& e) {
    SPDLOG_ERROR("BinanceHttpClient: Parse error: {}", e.what());
  }

  return snapshot;
}

}  // namespace herm
