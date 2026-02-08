#include "bybit_http_client.hpp"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>

namespace herm {

BaseHttpClient::DepthSnapshot BybitHttpClient::GetDepthSnapshot(
    const std::string& symbol, bool is_spot) {
  std::string host;
  
  // Check if custom base URL is set (for mocks/testing)
  if (!base_url_.empty()) {
    // Extract host from base_url (remove https:// prefix)
    host = base_url_;
    if (host.find("https://") == 0) {
      host = host.substr(8);
    } else if (host.find("http://") == 0) {
      host = host.substr(7);
    }
  } else {
    host = "api.bybit.com";
  }
  
  // Bybit V5 API uses category parameter: "spot" or "linear" (for perpetual)
  std::string category = is_spot ? "spot" : "linear";
  std::string path = "/v5/market/orderbook?category=" + category + "&symbol=" + symbol + "&limit=200";

  return FetchDepthSnapshot(host, path, "BybitHttpClient");
}

BaseHttpClient::DepthSnapshot BybitHttpClient::ParseDepthResponse(
    const std::string& json_response) {
  DepthSnapshot snapshot;

  try {
    auto json = nlohmann::json::parse(json_response);

    // Bybit V5 API format: {"retCode": 0, "result": {"s": "BTCUSDT", "b": [...], "a": [...], "u": 123}}
    if (json.contains("retCode") && json["retCode"].get<int>() != 0) {
      SPDLOG_ERROR("BybitHttpClient: API error code: {}", json["retCode"].get<int>());
      if (json.contains("retMsg")) {
        SPDLOG_ERROR("BybitHttpClient: Error message: {}", json["retMsg"].get<std::string>());
      }
      return snapshot;
    }

    if (!json.contains("result")) {
      SPDLOG_ERROR("BybitHttpClient: Invalid response format - missing result");
      return snapshot;
    }

    auto result = json["result"];

    // Extract update ID
    if (result.contains("u") && result["u"].is_number()) {
      snapshot.lastUpdateId = result["u"].get<int64_t>();
    }

    // Parse bids (array of [price, qty])
    if (result.contains("b") && result["b"].is_array()) {
      for (const auto& bid : result["b"]) {
        if (bid.is_array() && bid.size() >= 2) {
          double price = std::stod(bid[0].get<std::string>());
          double qty = std::stod(bid[1].get<std::string>());
          if (qty > 0.0) {
            snapshot.bids.emplace_back(price, qty);
          }
        }
      }
    }

    // Parse asks
    if (result.contains("a") && result["a"].is_array()) {
      for (const auto& ask : result["a"]) {
        if (ask.is_array() && ask.size() >= 2) {
          double price = std::stod(ask[0].get<std::string>());
          double qty = std::stod(ask[1].get<std::string>());
          if (qty > 0.0) {
            snapshot.asks.emplace_back(price, qty);
          }
        }
      }
    }

    // Sort bids descending, asks ascending
    std::sort(snapshot.bids.begin(), snapshot.bids.end(),
              [](const PriceLevel& a, const PriceLevel& b) { return a.price > b.price; });
    std::sort(snapshot.asks.begin(), snapshot.asks.end(),
              [](const PriceLevel& a, const PriceLevel& b) { return a.price < b.price; });

  } catch (const nlohmann::json::exception& e) {
    SPDLOG_ERROR("BybitHttpClient: JSON parse error: {}", e.what());
  } catch (const std::exception& e) {
    SPDLOG_ERROR("BybitHttpClient: Parse error: {}", e.what());
  }

  return snapshot;
}

}  // namespace herm
