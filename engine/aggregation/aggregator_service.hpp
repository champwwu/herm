#pragma once

#include "engine/common/config_manager.hpp"
#include "engine/common/event_thread.hpp"
#include "engine/common/rcu_ptr.hpp"
#include "engine/common/thread_safe_object_pool.hpp"
#include "market_data.grpc.pb.h"
#include "market_data_manager.hpp"
#include "order_book_processor.hpp"
#include <atomic>
#include <condition_variable>
#include <grpcpp/grpcpp.h>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace herm {
namespace market_data {

/**
 * @brief Per-subscriber state and message queue
 * 
 * Each subscriber has own writer thread and queued messages.
 * Uses object pool for zero-copy message passing.
 */
struct Subscriber {
  using PooledOrderBook = typename engine::common::ThreadSafeObjectPool<OrderBook>::PooledObject;
  
  // gRPC connection
  grpc::ServerWriter<OrderBook>* writer = nullptr;
  grpc::ServerContext* context = nullptr;
  
  // Subscription parameters
  std::string requested_symbol;
  RequestType request_type = RequestType::NONE;
  std::vector<int32_t> basis_points;
  std::vector<double> notional_volumes;
  
  // Message queue
  std::queue<PooledOrderBook> order_book_queue;
  std::mutex queue_mutex;
  std::condition_variable queue_cv;
  std::atomic<bool> active{true};
  
  // Writer thread
  std::thread writer_thread;
};

/**
 * @brief gRPC service for streaming aggregated market data
 * 
 * Manages connections to multiple exchanges, aggregates order books,
 * and streams filtered data to clients based on request type.
 * 
 * Architecture:
 * - MarketDataManager: connects to exchanges via WebSocket
 * - OrderBookProcessor: aggregates and filters order books
 * - Per-subscriber queues: decouples streaming from processing
 * 
 * Thread-safe with lock-free backpressure mechanism.
 */
class AggregatorServiceImpl final : public MarketDataService::Service {
 public:
  // Lifecycle
  AggregatorServiceImpl(const std::vector<std::string>& instrument_ids,
                       const engine::common::ConfigManager& config,
                       engine::common::EventThread* event_thread);
  ~AggregatorServiceImpl();
  
  // Control
  /** @brief Start exchange connections and processing loop */
  void Start();
  
  /** @brief Stop all connections and cancel streams */
  void Stop();
  
  // gRPC service interface
  /** @brief Stream filtered order books to client */
  grpc::Status StreamMarketData(grpc::ServerContext* context,
                                const StreamMarketDataRequest* request,
                                grpc::ServerWriter<OrderBook>* writer) override;
  
  // Query
  /** @brief Get order book by instrument ID (for REST API) */
  bool GetOrderBookByInstrumentId(const std::string& instrument_id, 
                                  OrderBook* output);
  
 private:
  // Configuration
  std::vector<std::string> instrument_ids_;
  const engine::common::ConfigManager& config_;
  engine::common::EventThread* event_thread_;
  std::unordered_set<std::string> valid_symbols_;  // O(1) validation
  
  // Components
  std::unique_ptr<MarketDataManager> market_data_manager_;
  std::unique_ptr<OrderBookProcessor> order_book_processor_;
  std::shared_ptr<engine::common::ThreadSafeObjectPool<OrderBook>> order_book_pool_;
  
  // Subscriber management (RCU pattern for lock-free reads)
  using SubscriberMap = std::map<uint64_t, std::shared_ptr<Subscriber>>;
  engine::common::RCUPtr<SubscriberMap> subscribers_;
  std::atomic<uint64_t> next_subscriber_id_{1};
  mutable std::mutex subscribers_write_mutex_;  // Only for writes
  
  // State
  std::atomic<bool> running_{false};
  std::atomic<bool> processing_active_{false};
  
  // Publishing
  /** @brief Send order book to matching subscribers */
  void PublishOrderBook(const std::string& symbol);
  
  /** @brief Per-subscriber writer thread */
  void WriterThread(uint64_t subscriber_id);
  
  /** @brief Remove and cleanup subscriber */
  void RemoveSubscriber(uint64_t subscriber_id);
  
  /** @brief Process pending updates (backpressure loop) */
  void ProcessPendingUpdates();
};

}  // namespace market_data
}  // namespace herm
