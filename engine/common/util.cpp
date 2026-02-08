#include "util.hpp"

namespace herm {
namespace engine {
namespace common {

ParsedUrl ParseWebSocketUrl(const std::string& url,
                            const std::string& default_host,
                            const std::string& default_port) {
  ParsedUrl result;
  result.host = default_host;
  result.port = default_port;
  result.path = "/";
  
  // Find protocol separator "://"
  size_t protocol_end = url.find("://");
  if (protocol_end == std::string::npos) {
    // No protocol, treat entire URL as host
    result.host = url;
    return result;
  }
  
  // Get everything after protocol
  std::string rest = url.substr(protocol_end + 3);
  
  // Find first slash to separate host:port from path
  size_t slash = rest.find('/');
  
  if (slash != std::string::npos) {
    // We have a path
    std::string host_port = rest.substr(0, slash);
    result.path = rest.substr(slash);
    
    // Parse host:port
    size_t colon = host_port.find(':');
    if (colon != std::string::npos) {
      result.host = host_port.substr(0, colon);
      result.port = host_port.substr(colon + 1);
    } else {
      result.host = host_port;
    }
  } else {
    // No path, just host:port
    size_t colon = rest.find(':');
    if (colon != std::string::npos) {
      result.host = rest.substr(0, colon);
      result.port = rest.substr(colon + 1);
    } else {
      result.host = rest;
    }
  }
  
  return result;
}

}  // namespace common
}  // namespace engine
}  // namespace herm
