#include "grpc_server.hpp"
#include <spdlog/spdlog.h>

namespace herm {
namespace engine {
namespace common {

GrpcServer::GrpcServer() = default;

GrpcServer::~GrpcServer() {
  Stop();
}

bool GrpcServer::Start(const std::string& address, 
                      grpc::Service* service,
                      bool enable_health_check) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (running_.load()) {
    SPDLOG_WARN("gRPC server is already running");
    return false;
  }

  if (!service) {
    SPDLOG_ERROR("Cannot start gRPC server: service is null");
    return false;
  }

  try {
    grpc::ServerBuilder builder;
    
    if (enable_health_check) {
      grpc::EnableDefaultHealthCheckService(true);
    }
    
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(service);
    
    server_ = builder.BuildAndStart();
    
    if (!server_) {
      SPDLOG_ERROR("Failed to start gRPC server on {}", address);
      return false;
    }
    
    running_.store(true);
    SPDLOG_INFO("gRPC server listening on {}", address);
    
    // Start wait thread
    wait_thread_ = std::thread(&GrpcServer::WaitThread, this);
    
    return true;
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Exception starting gRPC server: {}", e.what());
    return false;
  }
}

void GrpcServer::Stop() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!running_.load()) {
    return;  // Already stopped
  }
  
  running_.store(false);
  
  if (server_) {
    SPDLOG_DEBUG("Shutting down gRPC server...");
    server_->Shutdown();
    
    // Wait for wait thread to finish
    if (wait_thread_.joinable()) {
      wait_thread_.join();
    }
    
    server_.reset();
    SPDLOG_DEBUG("gRPC server shutdown complete");
  }
}

void GrpcServer::WaitThread() {
  if (server_) {
    server_->Wait();
    SPDLOG_DEBUG("gRPC server wait thread finished");
  }
}

}  // namespace common
}  // namespace engine
}  // namespace herm
