#include "event_thread.hpp"
#include "cpu_pinning.hpp"
#include <spdlog/spdlog.h>

namespace herm {
namespace engine {
namespace common {

//==============================================================================
// Lifecycle
//==============================================================================

EventThread::EventThread() {
}

EventThread::~EventThread() {
  Stop();
}

//==============================================================================
// Thread control
//==============================================================================

void EventThread::Start() {
  if (running_.exchange(true)) {
    return;  // Already running
  }
  thread_ = std::thread(&EventThread::Run, this);
}

bool EventThread::Stop(uint32_t timeout_ms) {
  if (!running_.exchange(false)) {
    return true;  // Already stopped
  }

  cv_.notify_all();
  
  if (thread_.joinable()) {
    if (timeout_ms == 0) {
      thread_.join();
      SPDLOG_INFO("EventThread stopped successfully");
      return true;
    }
    
    // Wait with timeout
    auto start = std::chrono::steady_clock::now();
    while (thread_.joinable()) {
      auto elapsed = std::chrono::steady_clock::now() - start;
      if (elapsed >= std::chrono::milliseconds(timeout_ms)) {
        SPDLOG_WARN("EventThread stop timeout after {}ms", timeout_ms);
        return false;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (!thread_.joinable()) {
        break;
      }
    }
    if (thread_.joinable()) {
      thread_.join();
    }
    SPDLOG_INFO("EventThread stopped successfully");
    return true;
  }
  return true;
}

//==============================================================================
// Task posting
//==============================================================================

void EventThread::Post(std::function<void()> task) {
  if (!running_.load()) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    task_queue_.push(std::move(task));
  }
  cv_.notify_one();
}

void EventThread::PostDelayed(std::function<void()> task, std::chrono::milliseconds delay) {
  if (!running_.load()) {
    return;
  }

  DelayedTask delayed_task;
  delayed_task.task = std::move(task);
  delayed_task.execute_at = std::chrono::steady_clock::now() + delay;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    delayed_tasks_.push(std::move(delayed_task));
  }
  cv_.notify_one();
}

int EventThread::SchedulePeriodic(std::function<void()> task, std::chrono::milliseconds interval) {
  if (!running_.load()) {
    return -1;
  }

  PeriodicTask periodic_task;
  periodic_task.id = next_task_id_++;
  periodic_task.task = std::move(task);
  periodic_task.interval = interval;
  periodic_task.next_run = std::chrono::steady_clock::now() + interval;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    periodic_tasks_.push(std::move(periodic_task));
  }
  cv_.notify_one();

  return periodic_task.id;
}

void EventThread::CancelPeriodic(int task_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  cancelled_periodic_tasks_[task_id] = true;
}

//==============================================================================
// Monitoring
//==============================================================================

size_t EventThread::GetQueueDepth() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return task_queue_.size() + delayed_tasks_.size() + periodic_tasks_.size();
}

//==============================================================================
// CPU affinity
//==============================================================================

void EventThread::SetCPUPinningManager(CPUPinningManager* manager) {
  cpu_pinning_manager_ = manager;
}

//==============================================================================
// Event loop (protected)
//==============================================================================

void EventThread::Run() {
  // Pin to CPU core if manager is set
  if (cpu_pinning_manager_) {
    try {
      cpu_pinning_manager_->PinCurrentThread("event_thread");
    } catch (const std::exception& e) {
      SPDLOG_ERROR("Failed to pin event_thread: {}", e.what());
      throw;
    }
  }
  
  while (running_.load()) {
    ProcessTasks();

    // Wait for tasks or timeout
    std::unique_lock<std::mutex> lock(mutex_);
    
    // Calculate next wake time
    auto now = std::chrono::steady_clock::now();
    auto next_wake = now + std::chrono::milliseconds(100);  // Default timeout
    
    if (!delayed_tasks_.empty()) {
      auto next_delayed = delayed_tasks_.top().execute_at;
      if (next_delayed < next_wake) {
        next_wake = next_delayed;
      }
    }
    
    if (!periodic_tasks_.empty()) {
      auto next_periodic = periodic_tasks_.top().next_run;
      if (next_periodic < next_wake) {
        next_wake = next_periodic;
      }
    }

    if (task_queue_.empty() && delayed_tasks_.empty() && periodic_tasks_.empty()) {
      cv_.wait(lock, [this] {
        return !running_.load() || !task_queue_.empty() || 
               !delayed_tasks_.empty() || !periodic_tasks_.empty();
      });
    } else if (next_wake > now) {
      cv_.wait_until(lock, next_wake, [this] {
        return !running_.load() || !task_queue_.empty();
      });
    }
  }

  // Process remaining tasks
  ProcessTasks();
}

void EventThread::ProcessTasks() {
  // Process immediate tasks
  while (true) {
    std::function<void()> task;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (task_queue_.empty()) {
        break;
      }
      task = std::move(task_queue_.front());
      task_queue_.pop();
    }

    try {
      task();
    } catch (const std::exception& e) {
      SPDLOG_ERROR("EventThread task exception: {}", e.what());
    }
  }

  // Process delayed tasks that are ready
  auto now = std::chrono::steady_clock::now();
  while (true) {
    std::function<void()> task;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (delayed_tasks_.empty() || delayed_tasks_.top().execute_at > now) {
        break;
      }
      task = std::move(delayed_tasks_.top().task);
      delayed_tasks_.pop();
    }

    try {
      task();
    } catch (const std::exception& e) {
      SPDLOG_ERROR("EventThread delayed task exception: {}", e.what());
    }
  }

  // Process periodic tasks that are ready
  while (true) {
    std::function<void()> task;
    std::chrono::milliseconds interval;
    int task_id;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (periodic_tasks_.empty() || periodic_tasks_.top().next_run > now) {
        break;
      }
      
      PeriodicTask periodic = periodic_tasks_.top();
      task_id = periodic.id;
      
      // Check if cancelled
      if (cancelled_periodic_tasks_.find(task_id) != cancelled_periodic_tasks_.end()) {
        periodic_tasks_.pop();
        cancelled_periodic_tasks_.erase(task_id);
        continue;
      }
      
      task = periodic.task;
      interval = periodic.interval;
      periodic_tasks_.pop();

      // Reschedule
      periodic.next_run = now + interval;
      periodic_tasks_.push(periodic);
    }

    try {
      task();
    } catch (const std::exception& e) {
      SPDLOG_ERROR("EventThread periodic task exception: {}", e.what());
    }
  }
}

}  // namespace common
}  // namespace engine
}  // namespace herm
