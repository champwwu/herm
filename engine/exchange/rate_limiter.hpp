#pragma once

#include <atomic>
#include <chrono>
#include <mutex>

namespace herm {

// Token bucket rate limiter for REST API calls
class RateLimiter {
 public:
  RateLimiter(int max_tokens, int refill_rate_per_second);
  ~RateLimiter() = default;

  // Non-copyable, movable
  RateLimiter(const RateLimiter&) = delete;
  RateLimiter& operator=(const RateLimiter&) = delete;
  RateLimiter(RateLimiter&&) = default;
  RateLimiter& operator=(RateLimiter&&) = default;

  // Try to acquire a token (non-blocking)
  // Returns true if token acquired, false otherwise
  bool TryAcquire();

  // Acquire a token (blocking until token available)
  void Acquire();

 private:
  void RefillTokens();
  std::chrono::milliseconds CalculateWaitTime() const;

  std::atomic<int> tokens_;
  int max_tokens_;
  int refill_rate_per_second_;
  std::chrono::steady_clock::time_point last_refill_;
  mutable std::mutex mutex_;
};

}  // namespace herm
