#pragma once

#include <mutex>
#include <string>
#include <vector>

namespace herm {
namespace engine {
namespace common {

class ConfigManager;

/**
 * @brief Manages CPU core allocation and thread pinning for performance isolation.
 * 
 * This class allows you to pin threads to specific CPU cores to reduce context
 * switching and improve cache locality. It reads core assignments from configuration
 * and distributes them to threads in a round-robin fashion.
 * 
 * Thread-safe: Can be called from multiple threads simultaneously.
 * Platform: Linux only (pthread_setaffinity_np).
 */
class CPUPinningManager {
 public:
  //=== Constructors & Destructor ===
  /**
   * @brief Constructs a CPUPinningManager with configuration reference.
   * @param config The configuration manager to read settings from.
   */
  explicit CPUPinningManager(const ConfigManager& config);
  
  /** @brief Default destructor. */
  ~CPUPinningManager() = default;

  //=== Deleted Copy Operations ===
  CPUPinningManager(const CPUPinningManager&) = delete;
  CPUPinningManager& operator=(const CPUPinningManager&) = delete;

  //=== Move Operations ===
  CPUPinningManager(CPUPinningManager&&) = default;
  CPUPinningManager& operator=(CPUPinningManager&&) = default;

  //=== Lifecycle ===
  /**
   * @brief Initializes the CPU pinning manager from configuration.
   * 
   * Reads `app.core_pinning.enable` and `app.core_pinning.cores` from config.
   * Validates that all specified cores exist on the system.
   * @throws std::runtime_error if configuration is invalid or cores don't exist.
   */
  void Initialize();

  //=== Thread Pinning ===
  /**
   * @brief Pins the current thread to the next available core.
   * 
   * Assigns cores in round-robin order from the configured list.
   * Thread-safe and can be called from multiple threads.
   * @param thread_name Descriptive name for logging purposes.
   * @throws std::runtime_error if no cores available or pinning fails.
   */
  void PinCurrentThread(const std::string& thread_name);

  //=== Query ===
  /** @brief Check if CPU pinning is enabled. */
  bool IsEnabled() const { return enabled_; }

  /** @brief Get number of available cores remaining in the pool. */
  size_t GetAvailableCoreCount() const;

 private:
  //=== Private Members ===
  const ConfigManager& config_;       ///< Reference to configuration
  bool enabled_{false};               ///< Whether CPU pinning is enabled
  std::vector<int> cores_;            ///< List of CPU cores to use
  size_t next_core_index_{0};         ///< Next core to assign (round-robin)
  mutable std::mutex mutex_;          ///< Protects next_core_index_

  //=== Private Helper Methods ===
  /**
   * @brief Gets next available core from the pool (caller must hold mutex).
   * @return Core ID to use.
   * @throws std::runtime_error if no cores available.
   */
  int GetNextAvailableCoreUnsafe();

  /**
   * @brief Pins current thread to specified core using pthread_setaffinity_np.
   * @param core_id The CPU core ID to pin to.
   * @param thread_name Descriptive name for logging.
   * @return True on success, false on failure.
   */
  bool PinThreadToCore(int core_id, const std::string& thread_name);

  /** @brief Validates that core ID exists on this system. */
  bool IsValidCore(int core_id) const;

  /** @brief Gets total number of CPU cores on this system. */
  int GetSystemCoreCount() const;
};

}  // namespace common
}  // namespace engine
}  // namespace herm
