#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace herm {
namespace engine {
namespace common {

class CPUPinningManager;

/**
 * @brief Single-threaded event loop for asynchronous task processing
 * 
 * Supports immediate, delayed, and periodic task execution.
 * Thread-safe task posting from any thread.
 */
class EventThread {
 public:
  // Lifecycle
  EventThread();
  virtual ~EventThread();

  // Non-copyable, movable
  EventThread(const EventThread&) = delete;
  EventThread& operator=(const EventThread&) = delete;
  EventThread(EventThread&&) = default;
  EventThread& operator=(EventThread&&) = default;

  // Thread control
  /** @brief Start event loop thread */
  void Start();
  
  /** @brief Stop event loop (timeout_ms=0 waits indefinitely) */
  bool Stop(uint32_t timeout_ms = 0);
  
  /** @brief Check if thread is running */
  bool IsRunning() const { return running_.load(); }

  // Task posting
  /** @brief Post task for immediate execution */
  void Post(std::function<void()> task);
  
  /** @brief Post task with delay */
  void PostDelayed(std::function<void()> task, std::chrono::milliseconds delay);
  
  /** @brief Schedule periodic task, returns task ID */
  int SchedulePeriodic(std::function<void()> task, std::chrono::milliseconds interval);
  
  /** @brief Cancel periodic task by ID */
  void CancelPeriodic(int task_id);

  // Monitoring
  /** @brief Get current queue depth (all task types) */
  size_t GetQueueDepth() const;

  // CPU affinity
  /** @brief Set CPU pinning manager (before Start()) */
  void SetCPUPinningManager(CPUPinningManager* manager);
  
  /** @brief Get CPU pinning manager */
  CPUPinningManager* GetCPUPinningManager() const { return cpu_pinning_manager_; }

 protected:
  // Task structures
  /** @brief Delayed task with execution time */
  struct DelayedTask {
    std::function<void()> task;
    std::chrono::steady_clock::time_point execute_at;

    bool operator<(const DelayedTask& other) const {
      return execute_at > other.execute_at;  // Min-heap
    }
  };

  /** @brief Periodic task with interval */
  struct PeriodicTask {
    int id;
    std::function<void()> task;
    std::chrono::milliseconds interval;
    std::chrono::steady_clock::time_point next_run;

    bool operator<(const PeriodicTask& other) const {
      return next_run > other.next_run;  // Min-heap
    }
  };

  // Event loop
  /** @brief Main event loop (runs in thread_) */
  virtual void Run();
  
  /** @brief Process ready tasks from all queues */
  void ProcessTasks();

  // State
  std::atomic<bool> running_{false};
  std::thread thread_;
  int next_task_id_{1};

  // Task queues
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::queue<std::function<void()>> task_queue_;
  std::priority_queue<DelayedTask, std::vector<DelayedTask>, std::less<DelayedTask>> delayed_tasks_;
  std::priority_queue<PeriodicTask, std::vector<PeriodicTask>, std::less<PeriodicTask>> periodic_tasks_;
  std::map<int, bool> cancelled_periodic_tasks_;

  // CPU pinning
  CPUPinningManager* cpu_pinning_manager_{nullptr};
};

}  // namespace common
}  // namespace engine
}  // namespace herm
