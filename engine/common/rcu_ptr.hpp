#pragma once

#include <atomic>
#include <memory>
#include <utility>

namespace herm {
namespace engine {
namespace common {

/**
 * @brief RCU (Read-Copy-Update) smart pointer using C++20 atomic shared_ptr
 * 
 * Provides lock-free reads for high-performance concurrent access.
 * Uses atomic shared_ptr operations to ensure memory safety through
 * reference counting.
 * 
 * Pattern:
 * - Readers: Lock-free atomic load, multiple concurrent readers
 * - Writers: Create new snapshot, atomic swap (serialized by caller)
 * - Memory: Automatically reclaimed when no readers hold references
 * 
 * Benefits:
 * - Wait-free reads (readers never block)
 * - Lock-free memory reclamation via shared_ptr refcount
 * - No ABA problem (addresses never reused)
 * - Cache-friendly (readers access immutable data)
 * 
 * Trade-offs:
 * - Memory overhead (temporary copies during updates)
 * - Write amplification (copy entire structure)
 * - Best for read-heavy workloads
 * 
 * @tparam T Type of object to protect (must be copyable/movable)
 */
template <typename T>
class RCUPtr {
 public:
  // Constructors
  
  /** @brief Default constructor - creates empty snapshot */
  RCUPtr() : ptr_(std::make_shared<T>()) {}
  
  /** @brief Constructor with initial value */
  explicit RCUPtr(std::shared_ptr<T> initial) : ptr_(std::move(initial)) {}
  
  /** @brief Constructor with in-place construction */
  template <typename... Args>
  explicit RCUPtr(Args&&... args) 
      : ptr_(std::make_shared<T>(std::forward<Args>(args)...)) {}
  
  // Non-copyable, non-movable (atomic shared_ptr is not movable)
  RCUPtr(const RCUPtr&) = delete;
  RCUPtr& operator=(const RCUPtr&) = delete;
  RCUPtr(RCUPtr&&) = delete;
  RCUPtr& operator=(RCUPtr&&) = delete;
  
  // Read operations (lock-free)
  
  /**
   * @brief Lock-free read - returns shared_ptr to current snapshot
   * 
   * Multiple readers can call this concurrently without blocking.
   * The returned shared_ptr keeps the snapshot alive even if a
   * writer updates the RCUPtr.
   * 
   * @return shared_ptr to immutable snapshot
   */
  std::shared_ptr<T> Read() const noexcept {
    return ptr_.load(std::memory_order_acquire);
  }
  
  /**
   * @brief Convenience operator for Read()
   * @return shared_ptr to current snapshot
   */
  std::shared_ptr<T> operator->() const noexcept {
    return Read();
  }
  
  // Write operations (caller must serialize)
  
  /**
   * @brief Update with new snapshot (atomic store)
   * 
   * Replaces current snapshot with new one. Old snapshot is
   * automatically deleted when last reader drops its reference.
   * 
   * IMPORTANT: Caller must serialize writes (use mutex if needed).
   * 
   * @param new_ptr New snapshot to publish
   */
  void Update(std::shared_ptr<T> new_ptr) noexcept {
    ptr_.store(std::move(new_ptr), std::memory_order_release);
  }
  
  /**
   * @brief Atomic exchange - returns old snapshot
   * 
   * Atomically replaces current snapshot and returns the old one.
   * Useful when you need to access the previous value.
   * 
   * @param new_ptr New snapshot to publish
   * @return Previous snapshot
   */
  std::shared_ptr<T> Exchange(std::shared_ptr<T> new_ptr) noexcept {
    return ptr_.exchange(std::move(new_ptr), std::memory_order_acq_rel);
  }
  
  /**
   * @brief Check if pointer is valid (not null)
   * @return true if snapshot exists
   */
  explicit operator bool() const noexcept {
    return ptr_.load(std::memory_order_acquire) != nullptr;
  }
  
 private:
  // C++20 native atomic shared_ptr - lock-free when possible
  std::atomic<std::shared_ptr<T>> ptr_;
};

}  // namespace common
}  // namespace engine
}  // namespace herm
