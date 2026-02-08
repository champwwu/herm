#pragma once

#include "market_data.grpc.pb.h"
#include <atomic>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace herm {
namespace engine {
namespace publishing {

/**
 * @brief Configuration for a single market data subscription
 * 
 * Specifies request type (BBO, price bands, volume bands) and parameters.
 */
struct StrategyConfig {
  std::string request_type;  ///< "BBO", "PriceBand", "VolumeBand", "None"
  std::string symbol_root;   ///< Symbol to subscribe (e.g., "BTCUSDT")
  std::vector<int32_t> basis_points;      ///< For PriceBand
  std::vector<double> notional_volumes;   ///< For VolumeBand
};

/**
 * @brief Runtime state for a single strategy stream
 * 
 * Owns gRPC stream thread and maintains latest received order book.
 * Thread-safe access via mutex.
 */
struct StrategyData {
  std::mutex mutex;                                ///< Protects latest_book
  herm::market_data::OrderBook latest_book;      ///< Cached order book
  std::thread stream_thread;                       ///< gRPC reader thread
  std::atomic<bool> active{true};                  ///< Stream active flag
  std::shared_ptr<grpc::ClientContext> grpc_context;  ///< For cancellation
};

}  // namespace publishing
}  // namespace engine
}  // namespace herm
