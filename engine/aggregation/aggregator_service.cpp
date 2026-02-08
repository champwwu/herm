#include "aggregator_service.hpp"
#include <spdlog/spdlog.h>

namespace herm {
namespace market_data {

AggregatorServiceImpl::AggregatorServiceImpl(
    const std::vector<std::string>& instrument_ids,
    const herm::engine::common::ConfigManager& config,
    herm::engine::common::EventThread* event_thread)
    : instrument_ids_(instrument_ids),
      config_(config),
      event_thread_(event_thread),
      subscribers_(),
      order_book_pool_(herm::engine::common::GetThreadSafeObjectPool<OrderBook>()) {
  
  // Initialize RCU ptr with empty map
  subscribers_.Update(std::make_shared<SubscriberMap>());
  
  // Build cache of valid symbols for fast O(1) validation
  for (const auto& instrument_id : instrument_ids) {
    size_t dot_pos = instrument_id.find('.');
    std::string symbol = (dot_pos != std::string::npos) ? 
                         instrument_id.substr(0, dot_pos) : instrument_id;
    valid_symbols_.insert(symbol);
  }
  
  // Create order book processor
  order_book_processor_ = std::make_unique<OrderBookProcessor>(instrument_ids, event_thread);
  
  // Create market data manager
  market_data_manager_ = std::make_unique<MarketDataManager>(instrument_ids, config, event_thread);
  
  // Set callback to forward updates to processor
  market_data_manager_->SetUpdateCallback([this](
      const std::string& exchange_name,
      const herm::engine::common::Instrument& instrument,
      const std::vector<herm::PriceLevel>& bids,
      const std::vector<herm::PriceLevel>& asks) {
    // Forward to processor
    order_book_processor_->OnOrderBookUpdate(exchange_name, instrument, bids, asks);
  });
  
  SPDLOG_INFO("AggregatorServiceImpl: Initialized with {} instruments ({} unique symbols)", 
              instrument_ids.size(), valid_symbols_.size());
}

AggregatorServiceImpl::~AggregatorServiceImpl() {
  Stop();
}

void AggregatorServiceImpl::Start() {
  if (running_.exchange(true)) {
    SPDLOG_WARN("AggregatorServiceImpl: Already running");
    return;
  }
  
  SPDLOG_INFO("AggregatorServiceImpl: Starting service");
  
  // Start market data manager (connects to exchanges)
  if (!market_data_manager_->Start()) {
    SPDLOG_ERROR("AggregatorServiceImpl: Failed to start market data manager");
  }
  
  // Start processing loop
  processing_active_.store(true, std::memory_order_release);
  if (event_thread_) {
    event_thread_->Post([this]() {
      ProcessPendingUpdates();
    });
    SPDLOG_INFO("AggregatorServiceImpl: Started processing loop");
  } else {
    SPDLOG_ERROR("AggregatorServiceImpl: Event thread not available");
  }
}

void AggregatorServiceImpl::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  
  SPDLOG_INFO("AggregatorServiceImpl: Stopping service");
  
  // Stop processing loop
  processing_active_.store(false, std::memory_order_release);
  
  // Cancel all active gRPC streams
  {
    auto subscribers_snapshot = subscribers_.Read();
    for (auto& [id, subscriber] : *subscribers_snapshot) {
      if (subscriber->context) {
        subscriber->context->TryCancel();
      }
      subscriber->active = false;
      subscriber->queue_cv.notify_all();
    }
  }
  
  // Wait for all writer threads
  {
    auto subscribers_snapshot = subscribers_.Read();
    for (auto& [id, subscriber] : *subscribers_snapshot) {
      if (subscriber->writer_thread.joinable()) {
        subscriber->writer_thread.join();
      }
    }
  }
  
  // Clear subscribers
  {
    std::lock_guard<std::mutex> lock(subscribers_write_mutex_);
    subscribers_.Update(std::make_shared<SubscriberMap>());
  }
  
  // Stop market data manager
  market_data_manager_->Stop();
  
  SPDLOG_INFO("AggregatorServiceImpl: Service stopped");
}

grpc::Status AggregatorServiceImpl::StreamMarketData(
    grpc::ServerContext* context,
    const StreamMarketDataRequest* request,
    grpc::ServerWriter<OrderBook>* writer) {
  
  SPDLOG_INFO("AggregatorServiceImpl::StreamMarketData called!");
  
  std::string requested_symbol = request->symbol();
  SPDLOG_INFO("AggregatorServiceImpl: StreamMarketData request for symbol: {}", requested_symbol);
  
  // Validate requested symbol using cached set (O(1) lookup)
  if (valid_symbols_.find(requested_symbol) == valid_symbols_.end()) {
    SPDLOG_WARN("AggregatorServiceImpl: Symbol not found: {}", requested_symbol);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Symbol not found");
  }
  
  // Extract request parameters
  RequestType request_type = request->request_type();
  std::vector<int32_t> basis_points(request->basis_points().begin(), 
                                    request->basis_points().end());
  std::vector<double> notional_volumes(request->notional_volumes().begin(), 
                                       request->notional_volumes().end());
  
  // Validate request parameters
  if (request_type == RequestType::PRICE_BAND && basis_points.empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, 
                       "PRICE_BAND requires basis_points");
  }
  if (request_type == RequestType::VOLUME_BAND && notional_volumes.empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, 
                       "VOLUME_BAND requires notional_volumes");
  }
  
  // Register subscriber
  uint64_t subscriber_id = next_subscriber_id_.fetch_add(1);
  auto subscriber = std::make_shared<Subscriber>();
  subscriber->writer = writer;
  subscriber->context = context;
  subscriber->requested_symbol = requested_symbol;
  subscriber->request_type = request_type;
  subscriber->basis_points = basis_points;
  subscriber->notional_volumes = notional_volumes;
  
  {
    std::lock_guard<std::mutex> lock(subscribers_write_mutex_);
    auto old_map = subscribers_.Read();
    auto new_map = std::make_shared<SubscriberMap>(*old_map);
    (*new_map)[subscriber_id] = subscriber;
    subscribers_.Update(new_map);
    SPDLOG_INFO("AggregatorServiceImpl: New subscriber {} for symbol {} (total subscribers: {})", 
                subscriber_id, requested_symbol, new_map->size());
  }
  
  // Send initial data if available
  if (order_book_processor_->HasPendingUpdate(requested_symbol)) {
    auto pooled_book = order_book_pool_->GetPooledObject();
    order_book_processor_->GetRequestedOrderBook(requested_symbol, request_type, basis_points, 
                                            notional_volumes, pooled_book.getPtr());
    {
      std::lock_guard<std::mutex> queue_lock(subscriber->queue_mutex);
      subscriber->order_book_queue.push(std::move(pooled_book));
      subscriber->queue_cv.notify_one();
    }
  }
  
  // Start writer thread
  subscriber->writer_thread = std::thread(&AggregatorServiceImpl::WriterThread, 
                                         this, subscriber_id);
  subscriber->writer_thread.detach();
  
  // Wait for context cancellation
  while (!context->IsCancelled() && subscriber->active.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  
  RemoveSubscriber(subscriber_id);
  SPDLOG_INFO("AggregatorServiceImpl: Subscriber {} disconnected", subscriber_id);
  
  return grpc::Status::OK;
}

bool AggregatorServiceImpl::GetOrderBookByInstrumentId(const std::string& instrument_id, 
                                                       OrderBook* output) {
  return order_book_processor_->GetOrderBookByInstrumentId(instrument_id, output);
}

void AggregatorServiceImpl::PublishOrderBook(const std::string& symbol) {
  // Lock-free read of subscriber map (RCU pattern)
  auto subscribers_snapshot = subscribers_.Read();
  
  int sent_count = 0;
  for (auto& [id, subscriber] : *subscribers_snapshot) {
    if (!(subscriber->writer && subscriber->active.load() && subscriber->requested_symbol == symbol)) {
      continue;
    }
    
    // Filter order book based on subscriber's request type (no lock held)
    auto pooled_book = order_book_pool_->GetPooledObject();
    order_book_processor_->GetRequestedOrderBook(symbol, subscriber->request_type, 
                                          subscriber->basis_points, 
                                          subscriber->notional_volumes, 
                                          pooled_book.getPtr());
    
    // Only lock subscriber's queue for push
    {
      std::lock_guard<std::mutex> queue_lock(subscriber->queue_mutex);
      subscriber->order_book_queue.push(std::move(pooled_book));
      subscriber->queue_cv.notify_one();
      sent_count++;
    }
  }
  
  if (sent_count > 0) {
    SPDLOG_TRACE("AggregatorServiceImpl: Sent order book to {} subscribers for symbol {}", 
                 sent_count, symbol);
  }
}

void AggregatorServiceImpl::ProcessPendingUpdates() {
  if (!running_.load(std::memory_order_acquire) || 
      !processing_active_.load(std::memory_order_acquire)) {
    return;
  }
  
  // Iterate through all symbols
  for (const auto& symbol : order_book_processor_->GetSymbols()) {
    // Check if there's a pending update (this atomically checks and clears the flag)
    if (order_book_processor_->HasPendingUpdate(symbol)) {
      PublishOrderBook(symbol);
      order_book_processor_->ClearInstrumentFlags(symbol);
    }
  }
  
  // Re-post to continue processing
  if (running_.load(std::memory_order_acquire) && 
      processing_active_.load(std::memory_order_acquire)) {
    event_thread_->Post([this]() {
      ProcessPendingUpdates();
    });
  }
}

void AggregatorServiceImpl::WriterThread(uint64_t subscriber_id) {
  std::shared_ptr<Subscriber> subscriber;
  {
    auto subscribers_snapshot = subscribers_.Read();
    auto it = subscribers_snapshot->find(subscriber_id);
    if (it == subscribers_snapshot->end() || !it->second->writer) {
      return;
    }
    subscriber = it->second;
  }
  
  SPDLOG_DEBUG("AggregatorServiceImpl: Writer thread started for subscriber {}", 
               subscriber_id);
  
  while (subscriber->active.load()) {
    Subscriber::PooledOrderBook pooled_book;
    {
      std::unique_lock<std::mutex> lock(subscriber->queue_mutex);
      subscriber->queue_cv.wait(lock, [&subscriber] {
        return !subscriber->order_book_queue.empty() || !subscriber->active.load();
      });
      
      if (!subscriber->active.load() && subscriber->order_book_queue.empty()) {
        break;
      }
      
      if (subscriber->order_book_queue.empty()) {
        continue;
      }
      
      pooled_book = std::move(subscriber->order_book_queue.front());
      subscriber->order_book_queue.pop();
    }
    
    // Safety check: verify pooled_book is valid before dereferencing
    if (!pooled_book) {
      SPDLOG_ERROR("AggregatorServiceImpl: Invalid (null) OrderBook in queue for subscriber {}", 
                   subscriber_id);
      continue;  // Skip this message and continue processing
    }
    
    if (subscriber->writer && !subscriber->writer->Write(*pooled_book)) {
      SPDLOG_WARN("AggregatorServiceImpl: Write failed for subscriber {}", subscriber_id);
      break;
    }
  }
  
  SPDLOG_DEBUG("AggregatorServiceImpl: Writer thread exiting for subscriber {}", 
               subscriber_id);
  RemoveSubscriber(subscriber_id);
}

void AggregatorServiceImpl::RemoveSubscriber(uint64_t subscriber_id) {
  std::lock_guard<std::mutex> lock(subscribers_write_mutex_);
  auto old_map = subscribers_.Read();
  auto it = old_map->find(subscriber_id);
  if (it != old_map->end()) {
    it->second->active = false;
    it->second->queue_cv.notify_all();
    
    // Create new map without this subscriber
    auto new_map = std::make_shared<SubscriberMap>(*old_map);
    new_map->erase(subscriber_id);
    subscribers_.Update(new_map);
  }
}

}  // namespace market_data
}  // namespace herm
