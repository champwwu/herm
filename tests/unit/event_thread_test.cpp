#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include "engine/common/event_thread.hpp"

using namespace herm::engine::common;

class EventThreadTest : public ::testing::Test {
protected:
  void SetUp() override {
    thread_ = std::make_unique<EventThread>();
  }

  void TearDown() override {
    if (thread_->IsRunning()) {
      thread_->Stop();
    }
  }

  std::unique_ptr<EventThread> thread_;
};

TEST_F(EventThreadTest, StartStop) {
  EXPECT_FALSE(thread_->IsRunning());
  
  thread_->Start();
  EXPECT_TRUE(thread_->IsRunning());
  
  thread_->Stop();
  EXPECT_FALSE(thread_->IsRunning());
}

TEST_F(EventThreadTest, PostTask) {
  thread_->Start();
  
  std::atomic<bool> executed{false};
  thread_->Post([&executed]() {
    executed = true;
  });
  
  // Wait for task to execute
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_TRUE(executed.load());
}

TEST_F(EventThreadTest, PostDelayedTask) {
  thread_->Start();
  
  std::atomic<bool> executed{false};
  auto start = std::chrono::steady_clock::now();
  
  thread_->PostDelayed([&executed]() {
    executed = true;
  }, std::chrono::milliseconds(100));
  
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  EXPECT_TRUE(executed.load());
  
  auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_GE(elapsed, std::chrono::milliseconds(100));
}

TEST_F(EventThreadTest, PeriodicTask) {
  thread_->Start();
  
  std::atomic<int> count{0};
  int task_id = thread_->SchedulePeriodic([&count]() {
    count++;
  }, std::chrono::milliseconds(50));
  
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  
  thread_->CancelPeriodic(task_id);
  int final_count = count.load();
  
  EXPECT_GE(final_count, 2);
}
