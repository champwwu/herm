// Demo gRPC client showcasing ApplicationKernel usage
// This demonstrates:
// - ApplicationKernel lifecycle
// - Config management
// - Event thread for async operations
// - Lazy thread for background tasks
// - gRPC client implementation

#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <atomic>
#include <chrono>
#include <thread>
#include <iomanip>
#include <sstream>
#include "application_kernel.hpp"
#include "demo.grpc.pb.h"

using namespace herm::engine::common;
using namespace herm::demo;

class DemoClientApp : public ApplicationKernel {
 public:
  DemoClientApp() {
    SetAppName("demo_client");
  }

 protected:
  void OnInitialize() override {
    SPDLOG_INFO("Demo client initializing...");
    // Load config
    server_address_ = GetConfig().GetString("server.target", "localhost:50052");
    request_interval_ms_ = GetConfig().GetInt("client.request_interval_ms", 2000);
    message_ = GetConfig().GetString("client.message", "Hello from demo client");
  }

  void OnStart() override {
    SPDLOG_INFO("Demo client connecting to {}", server_address_);
    
    // Create gRPC channel
    channel_ = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
    if (!channel_->WaitForConnected(
            std::chrono::system_clock::now() + std::chrono::seconds(10))) {
      SPDLOG_ERROR("Failed to connect to {}", server_address_);
      return;
    }
    
    stub_ = DemoService::NewStub(channel_);
    connected_ = true;
    
    // Example: Use worker thread for async connection notification
    Post([this]() {
      SPDLOG_INFO("Worker thread: Connected successfully to {}", server_address_);
    });
    
    // Example: Use worker thread for periodic requests
    SchedulePeriodic([this]() {
      if (connected_.load() && stub_) {
        SendEchoRequest();
      }
    }, std::chrono::milliseconds(request_interval_ms_));
    
    // Example: Use worker thread with delay
    PostDelayed([this]() {
      SPDLOG_INFO("Worker thread (delayed): Client fully operational");
    }, std::chrono::seconds(2));
    
    SPDLOG_INFO("Demo client started successfully");
  }

  void OnStop() override {
    SPDLOG_INFO("Demo client stopping...");
    connected_ = false;
    stub_.reset();
    channel_.reset();
    SPDLOG_INFO("Demo client sent {} total requests", request_count_.load());
  }

  void OnShutdown() override {
    SPDLOG_INFO("Demo client shutdown complete");
  }

 private:
  void SendEchoRequest() {
    EchoRequest request;
    request.set_message(message_ + " #" + std::to_string(request_count_.load() + 1));
    
    EchoResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->Echo(&context, request, &response);
    
    if (status.ok()) {
      request_count_++;
      
      // Convert timestamp to readable format
      auto timestamp = std::chrono::system_clock::time_point(
          std::chrono::microseconds(response.timestamp_us()));
      auto time_t = std::chrono::system_clock::to_time_t(timestamp);
      std::stringstream ss;
      ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
      
      SPDLOG_INFO("Echo response #{}: '{}' | Server timestamp: {} ({} us)", 
                   request_count_.load(), 
                   response.message(),
                   ss.str(),
                   response.timestamp_us());
    } else {
      SPDLOG_ERROR("Echo RPC failed: {}", status.error_message());
    }
  }

  std::string server_address_;
  std::string message_;
  int request_interval_ms_{2000};
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<DemoService::Stub> stub_;
  std::atomic<bool> connected_{false};
  std::atomic<int> request_count_{0};
};

int main(int argc, char** argv) {
  DemoClientApp app;
  return app.Run(argc, argv);
}
