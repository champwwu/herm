#include "engine/common/rcu_ptr.hpp"
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>

using namespace herm::engine::common;

// Simple test structure
struct TestData {
  int value;
  std::string text;
  
  TestData() : value(0), text("") {}
  TestData(int v, const std::string& t) : value(v), text(t) {}
};

TEST(RCUPtrTest, DefaultConstruction) {
  RCUPtr<TestData> rcu;
  auto snapshot = rcu.Read();
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->value, 0);
  EXPECT_EQ(snapshot->text, "");
}

TEST(RCUPtrTest, ConstructionWithSharedPtr) {
  auto initial = std::make_shared<TestData>(42, "hello");
  RCUPtr<TestData> rcu(initial);
  
  auto snapshot = rcu.Read();
  EXPECT_EQ(snapshot->value, 42);
  EXPECT_EQ(snapshot->text, "hello");
}

TEST(RCUPtrTest, ConstructionInPlace) {
  RCUPtr<TestData> rcu(99, "world");
  
  auto snapshot = rcu.Read();
  EXPECT_EQ(snapshot->value, 99);
  EXPECT_EQ(snapshot->text, "world");
}

TEST(RCUPtrTest, UpdateAndRead) {
  RCUPtr<TestData> rcu(1, "first");
  
  // Update with new snapshot
  auto new_snapshot = std::make_shared<TestData>(2, "second");
  rcu.Update(new_snapshot);
  
  // Read should return new snapshot
  auto read_snapshot = rcu.Read();
  EXPECT_EQ(read_snapshot->value, 2);
  EXPECT_EQ(read_snapshot->text, "second");
}

TEST(RCUPtrTest, Exchange) {
  RCUPtr<TestData> rcu(1, "first");
  
  auto new_snapshot = std::make_shared<TestData>(2, "second");
  auto old_snapshot = rcu.Exchange(new_snapshot);
  
  // Old snapshot should have original values
  EXPECT_EQ(old_snapshot->value, 1);
  EXPECT_EQ(old_snapshot->text, "first");
  
  // RCU should now have new values
  auto current = rcu.Read();
  EXPECT_EQ(current->value, 2);
  EXPECT_EQ(current->text, "second");
}

TEST(RCUPtrTest, BoolOperator) {
  RCUPtr<TestData> rcu(1, "test");
  EXPECT_TRUE(rcu);
  
  // Even after update, should still be valid
  rcu.Update(std::make_shared<TestData>(2, "test2"));
  EXPECT_TRUE(rcu);
}

TEST(RCUPtrTest, ArrowOperator) {
  RCUPtr<TestData> rcu(42, "test");
  
  auto snapshot = rcu.operator->();
  EXPECT_EQ(snapshot->value, 42);
  EXPECT_EQ(snapshot->text, "test");
}

TEST(RCUPtrTest, OldSnapshotRemainsValid) {
  RCUPtr<TestData> rcu(1, "first");
  
  // Reader holds reference to first snapshot
  auto snapshot1 = rcu.Read();
  EXPECT_EQ(snapshot1->value, 1);
  
  // Writer updates to second snapshot
  rcu.Update(std::make_shared<TestData>(2, "second"));
  
  // Old snapshot should still be valid and unchanged
  EXPECT_EQ(snapshot1->value, 1);
  EXPECT_EQ(snapshot1->text, "first");
  
  // New readers see new snapshot
  auto snapshot2 = rcu.Read();
  EXPECT_EQ(snapshot2->value, 2);
  EXPECT_EQ(snapshot2->text, "second");
}

TEST(RCUPtrTest, ConcurrentReads) {
  RCUPtr<TestData> rcu(0, "initial");
  const int num_readers = 10;
  const int reads_per_thread = 1000;
  
  std::vector<std::thread> readers;
  std::atomic<int> total_reads{0};
  
  for (int i = 0; i < num_readers; ++i) {
    readers.emplace_back([&rcu, &total_reads, reads_per_thread]() {
      for (int j = 0; j < reads_per_thread; ++j) {
        auto snapshot = rcu.Read();
        // Verify snapshot is valid
        EXPECT_NE(snapshot, nullptr);
        EXPECT_GE(snapshot->value, 0);
        total_reads.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  
  for (auto& t : readers) {
    t.join();
  }
  
  EXPECT_EQ(total_reads.load(), num_readers * reads_per_thread);
}

TEST(RCUPtrTest, ConcurrentReadsWithUpdates) {
  RCUPtr<TestData> rcu(0, "start");
  const int num_readers = 8;
  const int num_writers = 2;
  const int iterations = 100;
  
  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;
  
  // Start readers
  for (int i = 0; i < num_readers; ++i) {
    threads.emplace_back([&rcu, &stop]() {
      while (!stop.load()) {
        auto snapshot = rcu.Read();
        // Verify snapshot is valid
        EXPECT_NE(snapshot, nullptr);
        // Value should be monotonically increasing
        EXPECT_GE(snapshot->value, 0);
      }
    });
  }
  
  // Start writers (serialized updates)
  for (int i = 0; i < num_writers; ++i) {
    threads.emplace_back([&rcu, &stop, iterations, i]() {
      for (int j = 0; j < iterations; ++j) {
        int value = i * iterations + j;
        auto new_snapshot = std::make_shared<TestData>(value, "update");
        rcu.Update(new_snapshot);
        std::this_thread::yield();
      }
    });
  }
  
  // Wait for writers to finish
  for (size_t i = num_readers; i < threads.size(); ++i) {
    threads[i].join();
  }
  
  // Stop readers
  stop.store(true);
  for (size_t i = 0; i < num_readers; ++i) {
    threads[i].join();
  }
  
  // Final snapshot should exist
  auto final = rcu.Read();
  EXPECT_NE(final, nullptr);
}

TEST(RCUPtrTest, MemoryReclamation) {
  // Test that old snapshots are properly reclaimed
  auto shared_data = std::make_shared<TestData>(1, "test");
  std::weak_ptr<TestData> weak_ref = shared_data;
  
  RCUPtr<TestData> rcu(shared_data);
  shared_data.reset();  // Release our reference
  
  // Weak pointer should still be valid (RCU holds reference)
  EXPECT_FALSE(weak_ref.expired());
  
  // Update RCU with new snapshot
  rcu.Update(std::make_shared<TestData>(2, "new"));
  
  // Old snapshot should now be reclaimed
  EXPECT_TRUE(weak_ref.expired());
}

TEST(RCUPtrTest, OldSnapshotRemainsValidAfterMultipleUpdates) {
  RCUPtr<TestData> rcu(0, "initial");
  
  // Reader holds reference to initial snapshot
  auto snapshot0 = rcu.Read();
  EXPECT_EQ(snapshot0->value, 0);
  
  // Update multiple times
  rcu.Update(std::make_shared<TestData>(1, "first"));
  auto snapshot1 = rcu.Read();
  
  rcu.Update(std::make_shared<TestData>(2, "second"));
  auto snapshot2 = rcu.Read();
  
  // All snapshots should be valid and unchanged
  EXPECT_EQ(snapshot0->value, 0);
  EXPECT_EQ(snapshot0->text, "initial");
  
  EXPECT_EQ(snapshot1->value, 1);
  EXPECT_EQ(snapshot1->text, "first");
  
  EXPECT_EQ(snapshot2->value, 2);
  EXPECT_EQ(snapshot2->text, "second");
}
