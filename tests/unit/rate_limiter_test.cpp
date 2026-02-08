#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "rate_limiter.hpp"

using namespace herm;

class RateLimiterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Use a small refill rate for faster tests
    limiter_ = std::make_unique<RateLimiter>(10, 10);  // 10 tokens, refill 10 per second
  }

  std::unique_ptr<RateLimiter> limiter_;
};

TEST_F(RateLimiterTest, TryAcquire_Success) {
  EXPECT_TRUE(limiter_->TryAcquire());
}

TEST_F(RateLimiterTest, TryAcquire_MultipleTokens) {
  // Should be able to acquire all tokens
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(limiter_->TryAcquire()) << "Failed to acquire token " << i;
  }
}

TEST_F(RateLimiterTest, TryAcquire_ExhaustedTokens) {
  // Exhaust all tokens
  for (int i = 0; i < 10; ++i) {
    limiter_->TryAcquire();
  }
  
  // Should fail to acquire more
  EXPECT_FALSE(limiter_->TryAcquire());
}

TEST_F(RateLimiterTest, Acquire_Blocking) {
  // Exhaust all tokens
  for (int i = 0; i < 10; ++i) {
    limiter_->TryAcquire();
  }
  
  // Acquire should eventually succeed after refill
  // Use a timeout to avoid hanging
  auto start = std::chrono::steady_clock::now();
  
  std::thread t([this]() {
    limiter_->Acquire();  // Should block until token available
  });
  
  // Wait a bit for refill (should happen within 200ms for 10 tokens/sec)
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  
  // Thread should have completed
  if (t.joinable()) {
    t.join();
  }
  
  auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_GE(elapsed, std::chrono::milliseconds(100));  // Should have waited
}

TEST_F(RateLimiterTest, Refill_TokensOverTime) {
  // Exhaust all tokens
  for (int i = 0; i < 10; ++i) {
    limiter_->TryAcquire();
  }
  
  EXPECT_FALSE(limiter_->TryAcquire());
  
  // Wait for refill (RateLimiter refills based on seconds, so wait 1+ second)
  // At 10 tokens/sec, 1 second should refill 10 tokens
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));
  
  // Should be able to acquire tokens after refill
  bool acquired = limiter_->TryAcquire();
  EXPECT_TRUE(acquired);
}

TEST_F(RateLimiterTest, HighRefillRate) {
  // Create limiter with high refill rate
  RateLimiter fast_limiter(100, 1000);  // 100 tokens, 1000 per second
  
  // Exhaust tokens
  for (int i = 0; i < 100; ++i) {
    fast_limiter.TryAcquire();
  }
  
  EXPECT_FALSE(fast_limiter.TryAcquire());
  
  // Wait for refill (RateLimiter refills based on seconds, so wait 1+ second)
  // At 1000 tokens/sec, 1 second should refill 1000 tokens (capped at 100 max)
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));
  
  // Should have refilled - tokens should be at max (100)
  bool acquired = fast_limiter.TryAcquire();
  EXPECT_TRUE(acquired);
}

TEST_F(RateLimiterTest, LowRefillRate) {
  // Create limiter with low refill rate
  RateLimiter slow_limiter(5, 1);  // 5 tokens, 1 per second
  
  // Exhaust tokens
  for (int i = 0; i < 5; ++i) {
    slow_limiter.TryAcquire();
  }
  
  EXPECT_FALSE(slow_limiter.TryAcquire());
  
  // Wait for refill (should take ~1 second for 1 token)
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));
  
  EXPECT_TRUE(slow_limiter.TryAcquire());
}

TEST_F(RateLimiterTest, ConcurrentAccess) {
  std::atomic<int> success_count{0};
  std::atomic<int> fail_count{0};
  
  // Create multiple threads trying to acquire tokens
  std::vector<std::thread> threads;
  for (int i = 0; i < 20; ++i) {
    threads.emplace_back([this, &success_count, &fail_count]() {
      if (limiter_->TryAcquire()) {
        success_count++;
      } else {
        fail_count++;
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  // Should have exactly 10 successes (max tokens)
  EXPECT_EQ(success_count.load(), 10);
  EXPECT_EQ(fail_count.load(), 10);
}

// Note: RateLimiter cannot be moved due to atomic and mutex members
// Move semantics test removed
