#pragma once

#include <boost/pool/object_pool.hpp>
#include <cassert>
#include <memory>
#include <mutex>
#include <type_traits>

// Forward declaration for protobuf
namespace google {
namespace protobuf {
class Message;
}
}

namespace herm {
namespace engine {
namespace common {

/**
 * @brief Thread-safe object pool using boost::object_pool
 * 
 * Provides RAII wrappers for pooled objects with automatic return to pool.
 * Thread-safe allocation and deallocation via mutex protection.
 * 
 * @tparam T Type of objects to pool (must be default-constructible)
 */
template <typename T>
class ThreadSafeObjectPool : public std::enable_shared_from_this<ThreadSafeObjectPool<T>> {
 public:
  /**
   * @brief RAII wrapper for pooled objects
   * 
   * Automatically returns object to pool on destruction.
   * Move-only to prevent accidental copies.
   */
  class PooledObject {
   public:
    // Constructors
    /** @brief Default constructor - creates empty wrapper */
    PooledObject() : pool_(nullptr), ptr_(nullptr) {}
    
    /** @brief Internal constructor - wraps pooled object */
    explicit PooledObject(std::shared_ptr<ThreadSafeObjectPool<T>> pool, T* ptr)
        : pool_(pool), ptr_(ptr) {}

    /** @brief Destructor - returns object to pool */
    ~PooledObject() {
      if (ptr_ && pool_) {
        pool_->Return(ptr_);
      }
    }

    // Non-copyable
    PooledObject(const PooledObject&) = delete;
    PooledObject& operator=(const PooledObject&) = delete;

    // Move operations
    /** @brief Move constructor */
    PooledObject(PooledObject&& other) noexcept
        : pool_(std::move(other.pool_)), ptr_(other.ptr_) {
      other.ptr_ = nullptr;
    }

    /** @brief Move assignment */
    PooledObject& operator=(PooledObject&& other) noexcept {
      if (this != &other) {
        if (ptr_ && pool_) {
          pool_->Return(ptr_);
        }
        pool_ = std::move(other.pool_);
        ptr_ = other.ptr_;
        other.ptr_ = nullptr;
      }
      return *this;
    }

    // Accessors
    /** @brief Get raw pointer */
    T* getPtr() { return ptr_; }
    const T* getPtr() const { return ptr_; }
    
    // Pointer-like operators
    /** @brief Arrow operator for member access */
    T* operator->() { 
      assert(ptr_ != nullptr && "Dereferencing null PooledObject");
      return ptr_; 
    }
    
    const T* operator->() const { 
      assert(ptr_ != nullptr && "Dereferencing null PooledObject");
      return ptr_; 
    }
    
    /** @brief Dereference operator */
    T& operator*() { 
      assert(ptr_ != nullptr && "Dereferencing null PooledObject");
      return *ptr_; 
    }
    
    const T& operator*() const { 
      assert(ptr_ != nullptr && "Dereferencing null PooledObject");
      return *ptr_; 
    }
    
    /** @brief Boolean conversion - checks if pointer is valid */
    explicit operator bool() const { return ptr_ != nullptr; }

   private:
    std::shared_ptr<ThreadSafeObjectPool<T>> pool_;
    T* ptr_;
  };

  // Public interface
  /** @brief Acquire object from pool (cleared if protobuf) */
  PooledObject GetPooledObject() {
    std::lock_guard<std::mutex> lock(mutex_);
    T* ptr = pool_.construct();
    if (ptr && std::is_base_of<google::protobuf::Message, T>::value) {
      ptr->Clear();
    }
    return PooledObject(this->shared_from_this(), ptr);
  }

  /** @brief Release all pooled memory (optional cleanup) */
  void ReleaseMemory() {
    std::lock_guard<std::mutex> lock(mutex_);
    pool_.purge_memory();
  }

 private:
  friend class PooledObject;
  
  /** @brief Return object to pool (called by PooledObject destructor) */
  void Return(T* ptr) {
    if (ptr) {
      ptr->Clear();
      std::lock_guard<std::mutex> lock(mutex_);
      pool_.destroy(ptr);
    }
  }

  // Member variables
  boost::object_pool<T> pool_;
  mutable std::mutex mutex_;
};

/**
 * @brief Get singleton pool instance for type T
 * @return Shared pointer to thread-safe object pool
 */
template <typename T>
std::shared_ptr<ThreadSafeObjectPool<T>> GetThreadSafeObjectPool() {
  static auto pool = std::make_shared<ThreadSafeObjectPool<T>>();
  return pool;
}

}  // namespace common
}  // namespace engine
}  // namespace herm
