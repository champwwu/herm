#include "cpu_pinning.hpp"

#include "config_manager.hpp"
#include <spdlog/spdlog.h>
#include <sstream>
#include <stdexcept>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#endif

namespace herm {
namespace engine {
namespace common {

//=== Constructors ===
CPUPinningManager::CPUPinningManager(const ConfigManager& config)
    : config_(config) {
}

//=== Lifecycle ===

void CPUPinningManager::Initialize() {
  // Read CPU pinning configuration
  enabled_ = config_.GetBool("app.core_pinning.enable", false);
  
  if (!enabled_) {
    SPDLOG_INFO("CPU pinning is disabled");
    return;
  }

#ifndef __linux__
  SPDLOG_WARN("CPU pinning is only supported on Linux. Disabling CPU pinning.");
  enabled_ = false;
  return;
#endif

  // Read core list
  auto core_list_node = config_.GetNodeValue("app.core_pinning.cores");
  if (!core_list_node.is_array() || core_list_node.empty()) {
    throw std::runtime_error("CPU pinning enabled but 'app.core_pinning.cores' is not a valid non-empty array");
  }

  // Parse and validate cores
  for (const auto& core_node : core_list_node) {
    if (!core_node.is_number_integer()) {
      throw std::runtime_error("Invalid core ID in 'app.core_pinning.cores': must be integers");
    }
    int core_id = core_node.get<int>();
    
    if (!IsValidCore(core_id)) {
      std::ostringstream oss;
      oss << "Invalid core ID " << core_id << " in configuration. System has " 
          << GetSystemCoreCount() << " cores (0-" << (GetSystemCoreCount() - 1) << ")";
      throw std::runtime_error(oss.str());
    }
    
    cores_.push_back(core_id);
  }

  if (cores_.empty()) {
    throw std::runtime_error("CPU pinning enabled but no valid cores configured");
  }

  SPDLOG_INFO("CPU pinning initialized with {} cores: [{}]", 
              cores_.size(), 
              [this]() {
                std::ostringstream oss;
                for (size_t i = 0; i < cores_.size(); ++i) {
                  if (i > 0) oss << ", ";
                  oss << cores_[i];
                }
                return oss.str();
              }());
}

//=== Thread Pinning ===
void CPUPinningManager::PinCurrentThread(const std::string& thread_name) {
  if (!enabled_) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  
  int core_id = GetNextAvailableCoreUnsafe();
  
  if (!PinThreadToCore(core_id, thread_name)) {
    std::ostringstream oss;
    oss << "Failed to pin thread '" << thread_name << "' to core " << core_id;
    throw std::runtime_error(oss.str());
  }
}

//=== Query ===
size_t CPUPinningManager::GetAvailableCoreCount() const {
  if (!enabled_) {
    return 0;
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  return cores_.size() - next_core_index_;
}

//=== Private Helper Methods ===
int CPUPinningManager::GetNextAvailableCoreUnsafe() {
  if (next_core_index_ >= cores_.size()) {
    std::ostringstream oss;
    oss << "Insufficient CPU cores configured. Requested more threads than available cores. "
        << "Configured cores: " << cores_.size() << ", requested: " << (next_core_index_ + 1);
    throw std::runtime_error(oss.str());
  }
  
  return cores_[next_core_index_++];
}

bool CPUPinningManager::PinThreadToCore(int core_id, const std::string& thread_name) {
#ifdef __linux__
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  
  pthread_t current_thread = pthread_self();
  int result = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
  
  if (result != 0) {
    SPDLOG_ERROR("Failed to pin thread '{}' to core {}: error code {}", 
                 thread_name, core_id, result);
    return false;
  }
  
  SPDLOG_INFO("Successfully pinned thread '{}' to CPU core {}", thread_name, core_id);
  return true;
#else
  SPDLOG_WARN("CPU pinning not supported on this platform for thread '{}'", thread_name);
  return false;
#endif
}

bool CPUPinningManager::IsValidCore(int core_id) const {
  int num_cores = GetSystemCoreCount();
  return core_id >= 0 && core_id < num_cores;
}

int CPUPinningManager::GetSystemCoreCount() const {
#ifdef __linux__
  long num_cores = sysconf(_SC_NPROCESSORS_ONLN);
  if (num_cores <= 0) {
    SPDLOG_WARN("Failed to get system core count, assuming 1");
    return 1;
  }
  return static_cast<int>(num_cores);
#else
  return 1;
#endif
}

}  // namespace common
}  // namespace engine
}  // namespace herm
