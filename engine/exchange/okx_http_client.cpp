#include "okx_http_client.hpp"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>

namespace herm {

BaseHttpClient::DepthSnapshot OKXHttpClient::GetDepthSnapshot(
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
    host = "www.okx.com";
  }
  
  // OKX V5 API - same endpoint for spot and perp, symbol format differs
  // Spot: BTC-USDT, Perp: BTC-USDT-SWAP
  std::string path = "/api/v5/market/books?instId=" + symbol + "&sz=400";

  return FetchDepthSnapshot(host, path, "OKXHttpClient");
}

BaseHttpClient::DepthSnapshot OKXHttpClient::ParseDepthResponse(
    const std::string& json_response) {
  DepthSnapshot snapshot;

  try {
    auto json = nlohmann::json::parse(json_response);

    // OKX V5 API format: {"code": "0", "data": [{"asks": [...], "bids": [...], "ts": "..."}]}
    if (json.contains("code") && json["code"].get<std::string>() != "0") {
      SPDLOG_ERROR("OKXHttpClient: API error code: {}", json["code"].get<std::string>());
      return snapshot;
    }

    if (!json.contains("data") || !json["data"].is_array() || json["data"].empty()) {
      SPDLOG_ERROR("OKXHttpClient: Invalid response format");
      return snapshot;
    }

    auto data = json["data"][0];

    // Extract timestamp as lastUpdateId
    if (data.contains("ts") && data["ts"].is_string()) {
      snapshot.lastUpdateId = std::stoll(data["ts"].get<std::string>());
    }

    // Parse bids (array of [price, qty, liquidated_orders, orders_count])
    if (data.contains("bids") && data["bids"].is_array()) {
      for (const auto& bid : data["bids"]) {
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
    if (data.contains("asks") && data["asks"].is_array()) {
      for (const auto& ask : data["asks"]) {
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
    SPDLOG_ERROR("OKXHttpClient: JSON parse error: {}", e.what());
  } catch (const std::exception& e) {
    SPDLOG_ERROR("OKXHttpClient: Parse error: {}", e.what());
  }

  return snapshot;
}

}  // namespace herm
