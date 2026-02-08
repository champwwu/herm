#pragma once

#include <string>

namespace herm {
namespace engine {
namespace common {

// Parsed WebSocket URL components
struct ParsedUrl {
  std::string host;
  std::string port;
  std::string path;
};

// Parse a WebSocket URL (ws:// or wss://) into its components
// Example: "wss://stream.binance.com:9443/ws/btcusdt@depth"
//   -> host="stream.binance.com", port="9443", path="/ws/btcusdt@depth"
//
// If components are missing, uses provided defaults
ParsedUrl ParseWebSocketUrl(const std::string& url,
                            const std::string& default_host = "",
                            const std::string& default_port = "443");

}  // namespace common
}  // namespace engine
}  // namespace herm
