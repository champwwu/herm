/**
 * @file demo_server/main.cpp
 * @brief Demo application showcasing ApplicationKernel framework
 * 
 * Demonstrates:
 * - ApplicationKernel lifecycle (Initialize/Start/Stop/Shutdown)
 * - Config management (loading from JSON)
 * - EventThread for async task processing
 * - Periodic tasks (background status reporting)
 * - gRPC service implementation
 * 
 * This is a minimal example for learning the framework.
 * 
 * Usage:
 *   ./demo_server --config=config/demo_server.json
 */

#include "application_kernel.hpp"
#include "demo.grpc.pb.h"
#include <atomic>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <memory>
#include <string>
#include <thread>

using namespace herm::engine::common;
using namespace herm::demo;

/**
 * @brief Simple echo service for demonstration
 */
class DemoServiceImpl final : public DemoService::Service {
 public:
  /** @brief Echo message with timestamp */
  grpc::Status Echo(grpc::ServerContext* context,
                    const EchoRequest* request,
                    EchoResponse* response) override {
    (void)context;
    
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    
    response->set_message(request->message());
    response->set_timestamp_us(microseconds);
    
    request_count_++;
    SPDLOG_INFO("Echo request #{}: '{}' -> timestamp: {}", 
                 request_count_.load(), request->message(), microseconds);
    
    return grpc::Status::OK;
  }
  
  int GetRequestCount() const { return request_count_.load(); }
  
 private:
  std::atomic<int> request_count_{0};
};

/**
 * @brief Demo application using ApplicationKernel
 */
class DemoServerApp : public ApplicationKernel {
 public:
  DemoServerApp() {
    SetAppName("demo_server");
  }

 protected:
  void OnInitialize() override {
    SPDLOG_INFO("Demo server initializing...");
    // Load config
    std::string port = GetConfig().GetString("server.port", "50052");
    server_address_ = GetConfig().GetString("server.address", "0.0.0.0") + ":" + port;
  }

  void OnStart() override {
    SPDLOG_INFO("Demo server starting on {}", server_address_);
    
    service_ = std::make_unique<DemoServiceImpl>();
    
    // Example: Use worker thread for async operations
    Post([this]() {
      SPDLOG_INFO("Worker thread: Server ready to accept connections");
    });
    
    // Example: Use worker thread for periodic background tasks
    SchedulePeriodic([this]() {
      int count = service_->GetRequestCount();
      if (count > 0) {
        SPDLOG_INFO("Worker thread: Processed {} requests so far", count);
      }
    }, std::chrono::seconds(10));
    
    // Start gRPC server
    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(service_.get());
    
    server_ = builder.BuildAndStart();
    if (!server_) {
      throw std::runtime_error("Failed to start gRPC server");
    }
    
    SPDLOG_INFO("Demo gRPC server started successfully on {}", server_address_);
    
    // Run server in background thread
    server_thread_ = std::thread([this]() {
      if (server_) {
        server_->Wait();
      }
    });
  }

  void OnStop() override {
    SPDLOG_INFO("Demo server stopping...");
    if (server_) {
      server_->Shutdown();
    }
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
    service_.reset();
    SPDLOG_INFO("Demo server stopped. Total requests processed: {}", 
                 service_ ? service_->GetRequestCount() : 0);
  }

  void OnShutdown() override {
    SPDLOG_INFO("Demo server shutdown complete");
  }

 private:
  std::string server_address_;
  std::unique_ptr<DemoServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread server_thread_;
};

int main(int argc, char** argv) {
  DemoServerApp app;
  return app.Run(argc, argv);
}
