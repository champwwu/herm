#include "rate_limiter.hpp"
#include <thread>
#include <algorithm>

namespace herm {

RateLimiter::RateLimiter(int max_tokens, int refill_rate_per_second)
    : tokens_(max_tokens),
      max_tokens_(max_tokens),
      refill_rate_per_second_(refill_rate_per_second),
      last_refill_(std::chrono::steady_clock::now()) {}

bool RateLimiter::TryAcquire() {
  std::lock_guard<std::mutex> lock(mutex_);
  RefillTokens();
  if (tokens_.load() > 0) {
    tokens_--;
    return true;
  }
  return false;
}

void RateLimiter::Acquire() {
  while (!TryAcquire()) {
    auto wait_time = CalculateWaitTime();
    std::this_thread::sleep_for(wait_time);
  }
}

void RateLimiter::RefillTokens() {
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
      now - last_refill_).count();

  if (elapsed > 0) {
    int tokens_to_add = elapsed * refill_rate_per_second_;
    int current_tokens = tokens_.load();
    int new_tokens = std::min(max_tokens_, current_tokens + tokens_to_add);
    tokens_.store(new_tokens);
    last_refill_ = now;
  }
}

std::chrono::milliseconds RateLimiter::CalculateWaitTime() const {
  int current_tokens = tokens_.load();
  if (current_tokens >= max_tokens_) {
    return std::chrono::milliseconds(0);
  }
  
  // Calculate how long to wait for one token
  // tokens_needed = 1, refill_rate gives tokens per second
  int tokens_needed = 1;
  int milliseconds_to_wait = (tokens_needed * 1000 + refill_rate_per_second_ - 1) / refill_rate_per_second_;
  return std::chrono::milliseconds(milliseconds_to_wait);
}

}  // namespace herm
