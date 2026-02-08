#pragma once

#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include <mutex>

namespace herm {
namespace engine {
namespace common {

// Standardized gRPC server wrapper
// Manages gRPC server lifecycle and provides clean shutdown
class GrpcServer {
 public:
  GrpcServer();
  ~GrpcServer();

  // Non-copyable, movable
  GrpcServer(const GrpcServer&) = delete;
  GrpcServer& operator=(const GrpcServer&) = delete;
  GrpcServer(GrpcServer&&) = default;
  GrpcServer& operator=(GrpcServer&&) = default;

  // Start the gRPC server
  // address: server address (e.g., "0.0.0.0:50051")
  // service: gRPC service to register
  // enable_health_check: enable default health check service
  bool Start(const std::string& address, 
             grpc::Service* service,
             bool enable_health_check = true);

  // Stop the gRPC server gracefully
  // This will:
  // 1. Shutdown the server (stops accepting new connections)
  // 2. Wait for all active RPCs to complete
  // 3. Join the wait thread
  void Stop();

  // Check if server is running
  bool IsRunning() const { return running_.load(); }

  // Get the underlying gRPC server (for advanced use cases)
  grpc::Server* GetServer() { return server_.get(); }
  const grpc::Server* GetServer() const { return server_.get(); }

 private:
  std::unique_ptr<grpc::Server> server_;
  std::thread wait_thread_;
  std::atomic<bool> running_{false};
  std::mutex mutex_;
  
  void WaitThread();
};

}  // namespace common
}  // namespace engine
}  // namespace herm
