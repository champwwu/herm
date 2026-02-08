#include <iostream>
#include <iomanip>
#include <csignal>
#include <atomic>
#include <thread>
#include <chrono>
#include <algorithm>
#include <filesystem>
#include "application_kernel.hpp"
#include "engine/exchange/market_data_handler_factory.hpp"
#include "engine/common/instrument_registry.hpp"
#include "engine/common/config_manager.hpp"

using namespace herm::engine::common;

class HandlerClientApp : public ApplicationKernel {
 public:
  HandlerClientApp() {
    SetAppName("handler_client");
  }
  
  void SetInstrumentId(const std::string& instrument_id) {
    instrument_id_ = instrument_id;
  }

 protected:
  void OnStart() override {
    // Ensure stdout is unbuffered for real-time output
    std::cout.setf(std::ios::unitbuf);
    std::cerr.setf(std::ios::unitbuf);
    
    // Load instrument registry
    std::filesystem::path repo_root = std::filesystem::current_path();
    std::string instruments_csv_path = (repo_root / "static" / "instruments.csv").string();
    if (!GetInstrumentRegistry().LoadFromCSV(instruments_csv_path)) {
      std::cerr << "Failed to load instrument registry from " << instruments_csv_path << std::endl;
      return;
    }

    // Get instrument from registry
    const Instrument* instrument = GetInstrumentRegistry().GetInstrument(instrument_id_);
    if (!instrument) {
      std::cerr << "Instrument not found: " << instrument_id_ << std::endl;
      return;
    }

    std::cout << "Instrument: " << instrument_id_ << std::endl;
    std::cout << "  Exchange: " << instrument->exchange << std::endl;
    std::string type_str = (instrument->instrument_type == InstrumentType::SPOT) ? "SPOT" : "PERP";
    std::cout << "  Type: " << type_str << std::endl;
    std::cout << "  Symbol: " << instrument->GetSymbol() << std::endl;
    std::cout << std::endl;

    // Create handler using factory
    auto& factory = herm::MarketDataHandlerFactory::GetInstance();
    std::string handler_type = instrument->exchange;
    
    // Determine handler type based on exchange and instrument type
    if (handler_type == "binance") {
      if (instrument->instrument_type == InstrumentType::SPOT) {
        handler_type = "binance_spot";
      } else if (instrument->instrument_type == InstrumentType::PERP) {
        handler_type = "binance_futures";
      }
    }

    std::cout << "Creating handler for type: " << handler_type << std::endl;
    handler_ = factory.Create(handler_type, GetConfig());
    if (!handler_) {
      std::cerr << "Failed to create handler for type: " << handler_type << std::endl;
      return;
    }
    std::cout << "Handler created successfully" << std::endl;

    // Set update callback (now includes Instrument)
    handler_->SetUpdateCallback([this](const std::string& exchange,
                                      const Instrument& instrument,
                                      const std::vector<herm::PriceLevel>& bids,
                                      const std::vector<herm::PriceLevel>& asks) {
      OnOrderBookUpdate(exchange, instrument, bids, asks);
    });

    // Subscribe to instrument
    std::cout << "Subscribing to instrument..." << std::endl;
    if (!handler_->Subscribe(*instrument)) {
      std::cerr << "Failed to subscribe to instrument" << std::endl;
      return;
    }

    std::cout << "Connecting..." << std::endl;
    if (!handler_->Connect()) {
      std::cerr << "Failed to connect" << std::endl;
      return;
    }

    std::cout << "Connected! Waiting for order book updates..." << std::endl;
    std::cout << "Press Ctrl+C to exit" << std::endl;
    std::cout << std::endl;
  }

  void OnStop() override {
    SPDLOG_INFO("OnStop: Starting handler cleanup");
    if (handler_) {
      // Disconnect gracefully - this will wait for IO threads to exit (with timeout)
      SPDLOG_INFO("OnStop: Calling handler->Disconnect()");
      handler_->Disconnect();
      SPDLOG_INFO("OnStop: Disconnect complete, resetting handler");
      handler_.reset();  // Destroy handler (destructor will call Disconnect again, which is safe due to exchange())
      SPDLOG_INFO("OnStop: Handler destroyed");
    }
    spdlog::default_logger()->flush();
  }

 private:
  std::string instrument_id_;
  std::unique_ptr<herm::MarketDataHandler> handler_;

  void OnOrderBookUpdate(const std::string& exchange,
                         const Instrument& instrument,
                         const std::vector<herm::PriceLevel>& bids,
                         const std::vector<herm::PriceLevel>& asks) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;

    std::cout << "\n=== Order Book Update from " << exchange << " (" << instrument.instrument_id << ") ===" << std::endl;
    std::cout << "Time: " << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
    std::cout << "." << std::setfill('0') << std::setw(3) << ms.count() << std::endl;

    // Best bid and total bid levels
    if (!bids.empty()) {
      const auto& best_bid = bids.front();
      std::cout << "\nBest Bid: " << std::fixed << std::setprecision(2) << best_bid.price
                << " @ " << std::setprecision(8) << best_bid.quantity << std::endl;
      std::cout << "Total Bid Levels: " << bids.size() << std::endl;
    } else {
      std::cout << "\nBest Bid: N/A" << std::endl;
      std::cout << "Total Bid Levels: 0" << std::endl;
    }

    // Best ask and total ask levels
    if (!asks.empty()) {
      const auto& best_ask = asks.front();
      std::cout << "Best Ask: " << std::fixed << std::setprecision(2) << best_ask.price
                << " @ " << std::setprecision(8) << best_ask.quantity << std::endl;
      std::cout << "Total Ask Levels: " << asks.size() << std::endl;
    } else {
      std::cout << "Best Ask: N/A" << std::endl;
      std::cout << "Total Ask Levels: 0" << std::endl;
    }

    // Spread
    if (!bids.empty() && !asks.empty()) {
      double spread = asks[0].price - bids[0].price;
      double spread_bps = (spread / bids[0].price) * 10000.0;
      std::cout << "Spread: " << std::fixed << std::setprecision(2) << spread
                << " (" << std::setprecision(2) << spread_bps << " bps)" << std::endl;
    } else {
      std::cout << "Spread: N/A" << std::endl;
    }
    std::cout << std::endl;
    std::cout.flush();  // Force flush to ensure output is visible
  }
};

int main(int argc, char** argv) {
  // Parse arguments - skip --config_file and get instrument_id
  std::string instrument_id;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    // Skip config_file argument
    if (arg.find("--config_file") == 0) {
      continue;
    }
    // First non-config argument is the instrument_id
    if (instrument_id.empty()) {
      instrument_id = arg;
    }
  }

  if (instrument_id.empty()) {
    std::cerr << "Usage: " << argv[0] << " --config_file=<config> <instrument_id>" << std::endl;
    std::cerr << "Example: " << argv[0] << " --config_file=handler_client.json BTCUSDT.SPOT.BNC" << std::endl;
    return 1;
  }

  // Signal handlers are set up by ApplicationKernel
  HandlerClientApp app;
  app.SetInstrumentId(instrument_id);
  return app.Run(argc, argv);
}
