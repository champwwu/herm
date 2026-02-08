#include "base_http_client.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <spdlog/spdlog.h>

namespace herm {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = boost::asio::ip::tcp;

BaseHttpClient::DepthSnapshot BaseHttpClient::FetchDepthSnapshot(
    const std::string& host,
    const std::string& path,
    const std::string& exchange_name) {
  DepthSnapshot snapshot;

  try {
    // Create SSL context
    ssl::context ctx(ssl::context::tlsv12_client);
    ctx.set_default_verify_paths();

    // I/O context
    net::io_context ioc;

    // Resolver
    tcp::resolver resolver(ioc);
    auto const results = resolver.resolve(host, "443");

    // SSL stream
    ssl::stream<beast::tcp_stream> stream(ioc, ctx);
    SSL_set_tlsext_host_name(stream.native_handle(), host.c_str());

    // Connect
    beast::get_lowest_layer(stream).connect(results);

    // Handshake
    stream.handshake(ssl::stream_base::client);

    // Set up HTTP request
    http::request<http::string_body> req{http::verb::get, path, 11};
    req.set(http::field::host, host);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);

    // Send request
    http::write(stream, req);

    // Receive response
    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);

    // Parse response
    if (res.result() == http::status::ok) {
      snapshot = ParseDepthResponse(res.body());
    } else {
      SPDLOG_ERROR("{}: HTTP error {} for {}: {}", 
                   exchange_name, static_cast<int>(res.result()), path, res.body());
    }

    // Graceful shutdown
    beast::error_code ec;
    stream.shutdown(ec);
    // Ignore "stream truncated" and "not connected" errors - these are harmless
    if (ec && ec != beast::errc::not_connected) {
      SPDLOG_DEBUG("{}: Shutdown warning: {}", exchange_name, ec.message());
    }

  } catch (const std::exception& e) {
    SPDLOG_ERROR("{}: Exception fetching depth snapshot: {}", exchange_name, e.what());
  }

  return snapshot;
}

}  // namespace herm
