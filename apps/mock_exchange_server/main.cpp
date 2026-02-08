#include <iostream>
#include <string>
#include <filesystem>
#include <cstdlib>
#include "application_kernel.hpp"
#include "mock_exchange_websocket.hpp"

using namespace herm::engine::common;

class MockExchangeApp : public ApplicationKernel {
 public:
  MockExchangeApp() {
    SetAppName("mock_exchange");
  }

 protected:
  void OnStart() override {
    std::string exchange_name = GetConfig().GetString("exchange.name", "mock_exchange");
    std::string symbol = GetConfig().GetString("symbol", "BTCUSDT");
    std::string address = GetConfig().GetString("server.address", "0.0.0.0");
    uint16_t port = static_cast<uint16_t>(GetConfig().GetInt("server.port", 8080));
    double base_price = GetConfig().GetDouble("base_price", 50000.0);
    std::string fixture_file = GetConfig().GetString("fixture_file", "");
    
    SPDLOG_INFO("MockExchangeApp: Config loaded - exchange={}, symbol={}, address={}, port={}, fixture_file={}", 
                 exchange_name, symbol, address, port, fixture_file);
    
    server_ = std::make_unique<herm::MockExchangeWebSocketServer>(exchange_name, symbol, address, port, base_price);
    
    // Load fixture data if provided
    if (!fixture_file.empty()) {
      // Resolve relative path from repo root
      std::filesystem::path fixture_path(fixture_file);
      if (!fixture_path.is_absolute()) {
        // Get repo root (assume we're running from repo root)
        char* cwd = std::getenv("PWD");
        if (cwd) {
          fixture_path = std::filesystem::path(cwd) / fixture_path;
        } else {
          fixture_path = std::filesystem::absolute(fixture_path);
        }
      }
      SPDLOG_INFO("MockExchangeApp: Attempting to load fixture file: {} (resolved: {})", 
                   fixture_file, fixture_path.string());
      if (server_->LoadDeterminedOrderBookFromFile(fixture_path.string())) {
        SPDLOG_INFO("Loaded determined order book from fixture file: {}", fixture_path.string());
      } else {
        SPDLOG_WARN("Failed to load fixture file: {}", fixture_path.string());
      }
    } else {
      SPDLOG_WARN("MockExchangeApp: No fixture_file specified in config");
    }
    
    server_->Start();
    
    SPDLOG_INFO("Mock exchange server running on {}:{}", address, port);
  }

  void OnStop() override {
    if (server_) {
      server_->Stop();
    }
  }

 private:
  std::unique_ptr<herm::MockExchangeWebSocketServer> server_;
};

int main(int argc, char** argv) {
  MockExchangeApp app;
  return app.Run(argc, argv);
}
