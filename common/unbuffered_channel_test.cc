// Copyright: ThoughtSpot Inc 2017
// Author: Nipun Sehrawat (nipun@thoughtspot.com)

#include <time.h>

#include <future>
#include <memory>
#include <random>
#include <unordered_set>

#include "common/unbuffered_channel.h"
#include "glog/logging.h"
#include "glog/stl_logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::UnorderedElementsAreArray;

namespace thoughtspot {
namespace common {
namespace {

// A utility to generate random integers within specific ranges.
class RandomGen {
 public:
  RandomGen(int64_t seed = 0) : rng_(seed) {}
  int64_t NextInt(int lo, int hi) {
    std::uniform_int_distribution<int64_t> dist(lo, hi - 1);
    return dist(rng_);
  }

 private:
  std::mt19937_64 rng_;
};

// Verifies channels follow FIFO ordering.
TEST(UnbufferedChannel, FIFOOrdering) {
  UnbufferedChannel<int> ch;
  auto Read = [&ch]() {
    int got = 0;
    EXPECT_TRUE(ch.Read(&got));
    return got;
  };

  {
    auto Reader = std::async(std::launch::async, Read);
    ch.Write(1);
    EXPECT_EQ(1, Reader.get());
  }
  {
    auto Reader = std::async(std::launch::async, Read);
    ch.Write(2);
    EXPECT_EQ(2, Reader.get());
  }
  {
    auto Reader = std::async(std::launch::async, Read);
    ch.Write(3);
    EXPECT_EQ(3, Reader.get());
  }
}

// A test in which write of a value is issued before the corresponding read.
TEST(UnbufferedChannel, ReadAfterWrite) {
  UnbufferedChannel<int> ch;
  auto Reader = std::async(std::launch::async,
                      [&ch]() {
                        // Give time for the write to execute before this read.
                        usleep(50000);
                        int got = 0;
                        EXPECT_TRUE(ch.Read(&got));
                        return got;
                      });
  ch.Write(314);
  EXPECT_EQ(314, Reader.get());
}

// A test in which read for a value is issued before the corresponding read.
TEST(UnbufferedChannel, WriteAfterRead) {
  UnbufferedChannel<int> ch;
  auto Reader = std::async(std::launch::async,
                      [&ch]() -> int {
                        int got = 0;
                        EXPECT_TRUE(ch.Read(&got));
                        return got;
                      });
  // Give time for the read to execute before this write.
  usleep(50000);
  ch.Write(314);
  EXPECT_EQ(314, Reader.get());
}

// Verifies that read blocks if the channel is empty.
TEST(UnbufferedChannel, ReadBlocksUntilWrite) {
  UnbufferedChannel<int> ch;
  std::atomic<bool> signalled{false};
  auto Writer = std::async(std::launch::async,
                           [&ch, &signalled]() {
                             // Sleep for a while, to give the Read() an
                             // opportunity
                             // to proceed, if it is unblocked_writer.
                             usleep(1000);
                             signalled.store(true);
                             ch.Write(314);
                           });
  int got = 0;
  // This should block until a write.
  ASSERT_TRUE(ch.Read(&got));
  // Verifies that a Write(...) was issued before the above read could proceed.
  ASSERT_EQ(true, signalled.load());
  EXPECT_EQ(314, got);
}

// Verifies that write blocks untl there is a corresponding read.
TEST(UnbufferedChannel, WriteBlocksUntilRead) {
  UnbufferedChannel<int> ch;
  std::atomic<bool> signalled{false};
  auto Reader = std::async(std::launch::async,
                      [&ch, &signalled]() {
                        // Sleep for a while, to give the blocking Write() an
                        // opportunity to proceed, if it is unblocked_writer.
                        usleep(1000);
                        signalled.store(true);
                        int got = 0;
                        EXPECT_TRUE(ch.Read(&got));
                      });
  // This should block until a read.
  ch.Write(314);
  // Verifies that a Read(...) was issued before the above Write() could
  // proceed.
  ASSERT_EQ(true, signalled.load());
}

// Verifies that values written before the channel was closed can be read
// successfully.
TEST(UnbufferedChannel, Close) {
  UnbufferedChannel<int> ch;
  auto Reader = std::async(std::launch::async,
                      [&ch]() {
                        int got = 0;
                        EXPECT_TRUE(ch.Read(&got));
                        return got;
                      });
  ch.Write(1);
  ch.Close();
  EXPECT_EQ(1, Reader.get());
}

// Verifies that any excess values read after closing a channel will return the
// default value.
TEST(UnbufferedChannel, ReadAfterClose) {
  UnbufferedChannel<int> ch;
  auto Reader = std::async(std::launch::async,
                      [&ch]() {
                        int got = 0;
                        EXPECT_TRUE(ch.Read(&got));
                        return got;
                      });
  ch.Write(1);
  ch.Close();
  int got = 0;
  // Note that even though the channel has been closed, the sent value can be
  // read successfully. This is because the channel was closed after the value
  // was sent.
  EXPECT_EQ(1, Reader.get());

  // As all the sent values have already been read, reading more values will
  // return the default value for the element being read.
  EXPECT_FALSE(ch.Read(&got));
  EXPECT_EQ(int(), got);
  EXPECT_FALSE(ch.Read(&got));
  EXPECT_EQ(int(), got);
  EXPECT_FALSE(ch.Read(&got));
  EXPECT_EQ(int(), got);
}

TEST(UnbufferedChannel, Close_UnblocksAllReaders) {
  constexpr int kNumReaders = 10;
  constexpr auto duration = 24LL * 3600 * 1000000LL;  // 24 hrs.

  UnbufferedChannel<int> ch;
  std::vector<std::future<bool>> readers;
  for (int i = 0; i < kNumReaders; ++i) {
    readers.push_back(std::async(std::launch::async,
                                 [&ch, &duration, i]() {
                                   int v = 0;
                                   bool timedout = true;
                                   ch.Read(&v, duration, &timedout);
                                   return timedout;
                                 }));
  }
  // Unblock one reader by writing to the channel.
  ch.Write(1);
  // Unblock remaining by closing the channel.
  ch.Close();
  bool any_timedout = false;
  for (auto& r : readers) {
    any_timedout |= r.get();
  }
  // None fo the readers should timeout. If any of them timeout, this test will
  // hang for 24 hours.
  EXPECT_FALSE(any_timedout);
}

// Verifies that write() doesn't LOG(FATAL) on close() if the written value has
// been consumed by a read().
TEST(UnbufferedChannel, CloseAfterRead) {
  UnbufferedChannel<int> ch;
  auto f = std::async(std::launch::async,
                      [&ch] {
                        int value = 0;
                        // Note that Close() comes after Read(), so it is
                        // guaranteed that the
                        // written value has been read *before* the Close().
                        EXPECT_TRUE(ch.Read(&value));
                        ch.Close();
                        return value;
                      });
  ch.Write(1);
  EXPECT_EQ(1, f.get());
}

TEST(UnbufferedChannel, Correctness_MultipleWriters_SingleReader) {
  constexpr auto kNumWriters = 10;
  UnbufferedChannel<int> ch;
  std::vector<std::future<void>> writers;
  std::mutex m;
  std::condition_variable writer_unblocked;
  int unblocked_writer = -1;
  for (int i = 0; i < kNumWriters; ++i) {
    writers.emplace_back(
        std::async(std::launch::async,
                   [&ch, &unblocked_writer, &m, &writer_unblocked, i]() {
                     ch.Write(i);
                     std::unique_lock<std::mutex> l(m);
                     unblocked_writer = i;
                     writer_unblocked.notify_one();
                   }));
  }
  int writer = 0;
  ASSERT_TRUE(ch.Read(&writer));
  {
    std::unique_lock<std::mutex> l(m);
    writer_unblocked.wait(l, [&] { return unblocked_writer != -1; });
    // Verify that the writer that could proceed is indeed the one that got
    // unblocked.
    EXPECT_EQ(unblocked_writer, writer);
  }

  // Unblock the remaining writers and wait for them to finish.
  for (int i = 0; i < kNumWriters - 1; ++i) {
    int v = 0;
    EXPECT_TRUE(ch.Read(&v));
  }
  for (auto& w : writers) w.get();
}

// Empirically verifies that channel is thread safe in presence of multiple
// concurrent writers.
TEST(UnbufferedChannel, ThreadSafety_MultipleWriters_SingleReader) {
  constexpr auto kNumWriters = 10;
  constexpr auto kNumWrites = 10;
  UnbufferedChannel<int> ch;
  std::vector<std::future<void>> writers;
  for (int i = 0; i < kNumWriters; ++i) {
    writers.emplace_back(std::async(std::launch::async,
                                    [&ch, i]() {
                                      for (int j = 0; j < kNumWrites; ++j) {
                                        ch.Write(i * 10000 + j);
                                      }
                                    }));
  }
  auto Reader = std::async(std::launch::async,
                           [&ch]() {
                             // Read all the values.
                             std::set<int> read;
                             int v = 0;
                             while (ch.Read(&v)) {
                               read.insert(v);
                             }
                             return read;
                           });
  // Wait for all writers to finish and close the channel.
  for (auto& w : writers) w.get();
  ch.Close();

  // Wait for reader to finish.
  auto read = Reader.get();

  std::set<int> written;
  for (int i = 0; i < kNumWriters; ++i) {
    for (int j = 0; j < kNumWrites; ++j) {
      written.insert(i * 10000 + j);
    }
  }
  EXPECT_THAT(read, UnorderedElementsAreArray(written.begin(), written.end()));
}

// Empirically verifies that channel is thread safe in presence of multiple
// concurrent readers.
TEST(UnbufferedChannel, ThreadSafety_MultipleReaders_SingleWriter) {
  constexpr auto kNumReaders = 10;
  constexpr auto kNumReads = 10;
  std::set<int> written;
  UnbufferedChannel<int> ch;
  auto writer = std::async(std::launch::async,
                           [&ch, &written]() {
                             for (int i = 0; i < kNumReaders * kNumReads; ++i) {
                               ch.Write(i);
                               written.insert(i);
                             }
                           });

  std::vector<std::future<std::set<int>>> readers;
  for (int i = 0; i < kNumReaders; ++i) {
    readers.emplace_back(std::async(std::launch::async,
                                    [&ch, i]() {
                                      std::set<int> got;
                                      for (int j = 0; j < kNumReads; ++j) {
                                        int v = 0;
                                        CHECK(ch.Read(&v));
                                        got.insert(v);
                                      }
                                      return got;
                                    }));
  }
  std::set<int> read;
  // Wait for all readers to finish.
  for (auto& w : readers) {
    auto r = w.get();
    read.insert(r.begin(), r.end());
  }
  // Wait for the writer to finish.
  writer.get();

  EXPECT_THAT(read, UnorderedElementsAreArray(written.begin(), written.end()));
}

// Verifies that read times out in absence of a writer.
TEST(UnbufferedChannel, TimedWait_ReadTimeout) {
  UnbufferedChannel<int> ch;
  const int duration = 10000;
  int i = 0;
  bool timedout = false;
  EXPECT_FALSE(ch.Read(&i, duration, &timedout));
  EXPECT_TRUE(timedout);
}

// Verifies that write times out in absence of a reader.
TEST(UnbufferedChannel, TimedWait_WriteTimeout) {
  UnbufferedChannel<int> ch;
  const int duration = 10000;
  bool timedout = false;
  ch.Write(314, duration, &timedout);
  EXPECT_TRUE(timedout);
}

// Verifies that a timed read is cut short by a subsequent write.
TEST(UnbufferedChannel, TimedWait_Success) {
  UnbufferedChannel<int> ch;
  const auto duration = 24LL * 3600 * 1000000LL;  // 24 hrs.
  auto Reader = std::async(std::launch::async,
                           [&]() {
                             usleep(50000);
                             ch.Write(314);
                           });
  int i = 0;
  bool timedout = false;
  EXPECT_TRUE(ch.Read(&i, duration, &timedout));
  EXPECT_FALSE(timedout);
}

// Verifies that a timed read is cut short by a subsequent close.
TEST(UnbufferedChannel, TimedWait_ClosedInFlight) {
  UnbufferedChannel<int> ch;
  const auto duration = 24LL * 3600 * 1000000LL;  // 24 hrs.

  auto Closer = std::async(std::launch::async,
                      [&]() {
                        // Give enough time for the main thread to start
                        // reading.
                        usleep(250000);
                        ch.Close();
                      });
  DEFER(Closer.get());
  int i = 0;
  bool timedout = false;
  EXPECT_FALSE(ch.Read(&i, duration, &timedout));
  // Not a timeout as the wait is cut short by the closing.
  EXPECT_FALSE(timedout);
}

// Verifies that a timed read on an already closed channel returns immediately.
TEST(UnbufferedChannel, TimedWait_AlreadyClosed) {
  UnbufferedChannel<int> ch;
  // This test will end up waiting for 24hrs (and thus timeout) if an already
  // the imeplementation ends up waiting on a closed channel as well.
  const auto duration = 24LL * 3600 * 1000000LL;  // 1 day.
  ch.Close();
  int i = 0;
  bool timedout = false;
  EXPECT_FALSE(ch.Read(&i, duration, &timedout));
  EXPECT_FALSE(timedout);
}

TEST(UnbufferedChannel, TimedWait_ExpensiveCopying) {
  // Large enough to ensure that the asyn writer thread gets to run before the
  // reader times out.
  const auto kTimeoutDuration = 100000LL;
  // Much larger than timeout.
  const auto kCopyingDuration = 2 * kTimeoutDuration;
  class BigClass {
   public:
    BigClass() = default;
    BigClass(const BigClass& other) {
      usleep(kCopyingDuration);
      i = other.i;
    }
    BigClass& operator=(const BigClass& other) {
      usleep(kCopyingDuration);
      i = other.i;
      return *this;
    }
    int i = 0;
  };
  UnbufferedChannel<BigClass> ch;
  BigClass sent;
  sent.i = 314;
  auto Writer = std::async(std::launch::async,
                           [&ch, &sent, kTimeoutDuration]() {
                             bool timedout = true;
                             ch.Write(sent, kTimeoutDuration, &timedout);
                             return timedout;
                           });
  BigClass got;
  bool timedout = true;
  EXPECT_TRUE(ch.Read(&got, kTimeoutDuration, &timedout));
  // The operation shouldn't timeout, even though it overall took more than
  // kTimeoutDuration. This is because the handshake happened within the timeout
  // limit, but the exchange of value took much longer.
  EXPECT_FALSE(timedout);
  EXPECT_EQ(got.i, sent.i);
}

TEST(UnbufferedChannel, Stress) {
  UnbufferedChannel<int> c;
  const int64_t seed = time(nullptr);
  LOG(INFO) << "Seed: " << seed;
  RandomGen rng(seed);
  const int num_ops = rng.NextInt(79, 191);

  const int num_consumers = rng.NextInt(7, 23);
  const int num_producers = rng.NextInt(11, 31);
  std::vector<int> producer_ops(num_producers, 1);
  std::vector<int> consumer_ops(num_consumers, 1);
  // Assign random amount of work to each producer.
  {
    for (int sum = 0; sum < num_ops - num_producers; ++sum) {
      producer_ops[rng.NextInt(0, num_producers)]++;
    }
  }
  // Assign random amount of work to each consumer.
  {
    for (int sum = 0; sum < num_ops - num_consumers; ++sum) {
      consumer_ops[rng.NextInt(0, num_consumers)]++;
    }
  }

  auto Produce = [&c](int num) {
    for (int i = 0; i < num; ++i) {
      c.Write(1);
    }
  };

  std::atomic<int64_t> global_sum{0};
  auto Consume = [&c, &global_sum](int num) {
    int local_sum = 0;
    for (int i = 0; i < num; ++i) {
      int v = 0;
      EXPECT_TRUE(c.Read(&v));
      local_sum += v;
    }
    global_sum.fetch_add(local_sum);
  };

  LOG(INFO) << "Launching " << num_producers << " producers and "
            << num_consumers << " consumers";
  std::vector<std::future<void>> producers;
  for (int i = 0; i < num_producers; ++i) {
    producers.push_back(
        std::async(std::launch::async, Produce, producer_ops[i]));
  }

  std::vector<std::future<void>> consumers;
  for (int i = 0; i < num_consumers; ++i) {
    consumers.push_back(
        std::async(std::launch::async, Consume, consumer_ops[i]));
  }

  // Barrier: Wait for producers and consumers to terminate.
  for (int i = 0; i < num_producers; ++i) {
    producers[i].get();
  }
  for (int i = 0; i < num_consumers; ++i) {
    consumers[i].get();
  }
  EXPECT_EQ(num_ops, global_sum.load());
}

// A stress test that asserts that the set of values that were sent on the
// channel is same as the set of values received on the channel. Note that this
// test doesn't check of ordering on the received values.
TEST(UnbufferedChannel, Stress_SetEquality) {
  UnbufferedChannel<int> c;
  const int64_t seed = time(nullptr);
  LOG(INFO) << "Seed: " << seed;
  RandomGen rng(seed);
  const int num_ops = rng.NextInt(79, 378);
  LOG(INFO) << "Num operations: " << num_ops;

  const int num_consumers = rng.NextInt(7, 23);
  const int num_producers = rng.NextInt(11, 31);
  std::vector<int> producer_ops(num_producers, 1);
  std::vector<int> consumer_ops(num_consumers, 1);
  // Assign random amount of work to each producer.
  {
    for (int sum = 0; sum < num_ops - num_producers; ++sum) {
      producer_ops[rng.NextInt(0, num_producers)]++;
    }
  }
  // Assign random amount of work to each consumer.
  {
    for (int sum = 0; sum < num_ops - num_consumers; ++sum) {
      consumer_ops[rng.NextInt(0, num_consumers)]++;
    }
  }

  // Each producer produces a stride of numbers that doesn't override with other
  // producer's strides.
  constexpr int kStride = 1000000;
  std::vector<std::unordered_set<int>> sent_nums(num_producers);
  auto Produce = [&c, &sent_nums](int idx, int num) {
    for (int i = 0; i < num; ++i) {
      const int num = idx * kStride + i;
      c.Write(num);
      sent_nums[idx].insert(num);
    }
  };

  std::vector<std::unordered_set<int>> received_nums(num_consumers);
  auto Consume = [&c, &received_nums](int idx, int num) {
    for (int i = 0; i < num; ++i) {
      int v = 0;
      EXPECT_TRUE(c.Read(&v));
      received_nums[idx].insert(v);
    }
  };

  LOG(INFO) << "Launching " << num_producers << " producers and "
            << num_consumers << " consumers";
  std::vector<std::future<void>> producers;
  for (int i = 0; i < num_producers; ++i) {
    producers.push_back(
        std::async(std::launch::async, Produce, i, producer_ops[i]));
  }

  std::vector<std::future<void>> consumers;
  for (int i = 0; i < num_consumers; ++i) {
    consumers.push_back(
        std::async(std::launch::async, Consume, i, consumer_ops[i]));
  }

  // Barrier: Wait for producers and consumers to terminate.
  for (int i = 0; i < num_producers; ++i) {
    producers[i].get();
  }
  for (int i = 0; i < num_consumers; ++i) {
    consumers[i].get();
  }

  std::set<int> sent;
  std::set<int> received;
  for (const auto& elem : sent_nums) {
    sent.insert(elem.begin(), elem.end());
  }
  for (const auto& elem : received_nums) {
    received.insert(elem.begin(), elem.end());
  }
  // Check that sent and received sets are the same.
  EXPECT_THAT(sent, ::testing::ContainerEq(received));
  // As unique numbers are sent, number of sent (and received) values should be
  // the same as the number of operations.
  EXPECT_EQ(num_ops, sent.size());
}

}  // namespace
}  // namespace common
}  // namespace thoughtspot
