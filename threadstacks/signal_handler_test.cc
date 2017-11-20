// Copyright: ThoughtSpot Inc 2017
// Author: Nipun Sehrawat (nipun@thoughtspot.com)

#include "threadstacks/signal_handler.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/select.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <future>
#include <random>
#include <thread>

#include "common/defer.h"
#include "common/sysutil.h"
#include "common/unbuffered_channel.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "glog/logging.h"

using testing::AllOf;
using testing::Gt;
using testing::IsEmpty;
using testing::Le;
using thoughtspot::common::UnbufferedChannel;

namespace thoughtspot {
namespace {

int64_t GetTid() { return syscall(SYS_gettid); }

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

// Extracts tids out of the results in @r and return them.
std::vector<pid_t> GetTids(const std::vector<StackTraceCollector::Result>& r) {
  std::vector<pid_t> tids;
  for (const auto& e : r) {
    tids.insert(tids.end(), e.tids.begin(), e.tids.end());
  }
  return tids;
}

void F(std::future<void> f) {
  std::cout << "F(): Start" << std::endl;
  DEFER(std::cout << "F(): End" << std::endl);
  f.get();
}

void G(int fd) {
  std::cout << "G(): Start" << std::endl;
  DEFER(std::cout << "G(): End" << std::endl);
  char ch;
  while (0 != read(fd, &ch, sizeof(ch))) {;}
}

// Returns the bytes read from @fd as a string.
std::string ReadFromFD(int fd) {
  std::string output;
  // Read from the file descriptor, until told to exit.
  while (true) {
    char buf[1000];
    auto num_read = read(fd, &buf, sizeof(buf));
    if (-1 == num_read) {
      std::cerr << "Failed to read from pipe" << std::endl;  // errno
    } else if (num_read == 0) {
      break;
    } else if (num_read > 0) {
      output.append(std::string(buf, num_read));
    }
  }
  return output;
}

// Returns number of occurrences of @token in @str.
int NumMatches(const std::string& str, const std::string& token) {
  int count = -1;
  size_t pos = -1;
  do {
    ++count;
    pos = str.find(token, pos + 1);
  } while (std::string::npos != pos);
  return count;
}

class StackTraceCollectorTest : public ::testing::Test {
 public:
  StackTraceCollectorTest() = default;
  ~StackTraceCollectorTest() override = default;
  void SetUp() override {
    // Wait for any threads from the previous test to stop appearing in the
    // ListThread's output. Proceed only when there are precisely two threads
    // detected - the main thread and the external stack trace signal processor
    // thread.
    while (2 != common::Sysutil::ListThreads().size()) {;}
    // Note down the minimum available file descriptor before the test starts.
    min_fd_init_ = open("/dev/null", O_WRONLY);
    close(min_fd_init_);
  }
  void TearDown() override {
    int min_fd_final = open("/dev/null", O_WRONLY);
    close(min_fd_final);
    EXPECT_EQ(min_fd_init_, min_fd_final)
        << "File descriptor leakage detected! Minimum file descriptor before "
        << "start of the test: " << min_fd_init_
        << ", and after the test: " << min_fd_final;
  }

 private:
  int min_fd_init_ = 0;
};

// Tests direct usage of StackTraceCollector. Mimics usage pattern of stack
// trace collection handler.
TEST_F(StackTraceCollectorTest, Basic) {
  StackTraceCollector collector;
  std::string error;
  auto ret = collector.Collect(&error);
  EXPECT_THAT(error, IsEmpty());
  EXPECT_THAT(
      GetTids(ret),
      ::testing::UnorderedElementsAreArray(common::Sysutil::ListThreads()));
}

// Verifies for system sanity when many threads bombard the process with
// external stack collection signal using kill().
TEST_F(StackTraceCollectorTest, Stress_ExternalSignum_Kill) {
  const int kNumSignallers = 10;
  const int kNumIterations = 5;
  // Step 1: Redirect stderr to a pipe. This helps in verifying the contents
  // of stderr.
  const int stderr_copy = dup(STDERR_FILENO);
  ASSERT_NE(-1, stderr_copy);
  NAMED_DEFER(restore_stderr,
              // Restored stderr at the end of test.
              dup2(stderr_copy, STDERR_FILENO);
              close(stderr_copy););
  int stderr_pipe[2];
  ASSERT_NE(-1, pipe(stderr_pipe));
  DEFER(close(stderr_pipe[0]));
  DEFER(close(stderr_pipe[1]));
  ASSERT_NE(-1, dup2(stderr_pipe[1], STDERR_FILENO));
  close(stderr_pipe[1]);  // stderr_pipe[1] is now an alias for STDERR_FILENO.

  // Step 2: Launch t1 and t2 threads.
  int t1_done[2];  // Used to signal t1 to exit.
  DEFER(close(t1_done[0]));
  DEFER(close(t1_done[1]));
  ASSERT_NE(-1, pipe(t1_done)) << "Errno: " << errno;
  std::promise<void> t2_done;  // Used to signal t2 to exit.
  std::thread t1(F, t2_done.get_future());
  std::thread t2(G, t1_done[0]);

  // Step 3: Launch a task that reads from stderr pipe until the stderr pipe
  // is closed. The task returns the stderr data in @stderr_output future.
  std::future<std::string> stderr_output =
      std::async(std::launch::async, ReadFromFD, stderr_pipe[0]);

  // Step 4: Wait for all threads to start and capture their tids. Then,
  // launch @kNumSignallers child processes, which send @kNumIterations
  // external signal using sigqueue().
  while (5 != common::Sysutil::ListThreads().size()) {;}
  const auto tids = common::Sysutil::ListThreads();

  std::set<pid_t> child;
  for (int i = 0; i < kNumSignallers; ++i) {
    auto pid = fork();
    ASSERT_NE(-1, pid) << "Failed to fork";
    // Child process.
    if (pid == 0) {
      RandomGen rng;
      for (int i = 0; i < kNumIterations; ++i) {
        // Send external signal to a randomly selected thread.
        auto tid = tids[rng.NextInt(0, tids.size())];
        kill(tid, StackTraceSignal::ExternalSignum());
      }
      exit(0);
    } else {
      child.insert(pid);
    }
  }

  // Wait for all child processes to finish.
  while (not child.empty()) {
    int status = 0;
    auto pid = waitpid(*child.begin(), &status, 0);
    if (pid > 0) {
      child.erase(pid);
    }
  }

  // Give enough time for all signals to be delivered. Unfortunately there is
  // no easy way to write this test without a sleep, given than signals are
  // delivered out-of-band.
  int sleep_time = 3;
  while (sleep_time > 0) {
    sleep_time = sleep(sleep_time);
  }

  // Step 5: Verify expectations.
  // Redirect stderr back to STDERR_FILNO. Note that this also closes the
  // write end of stderr_pipe, which terminates the @stderr_output task.
  restore_stderr.run_and_expire();

  // Signal t1 and t2 to exit.
  close(t1_done[1]);
  t2_done.set_value();
  t1.join();
  t2.join();

  // Verify that stderr output is as expected.
  auto stderr_str = stderr_output.get();
  auto num_starts = NumMatches(stderr_str, "Stack traces - Start");
  auto num_ends = NumMatches(stderr_str, "Stack traces - End");
  EXPECT_EQ(num_starts, num_ends) << stderr_str;
  EXPECT_THAT(num_starts, AllOf(Gt(0), Le(kNumSignallers * kNumIterations)))
      << stderr_str;
  EXPECT_EQ(
      0, NumMatches(stderr_str, "Failed to get all stacktraces within timeout"))
      << stderr_str;
}

// Verifies for system sanity when many threads bombard the process with
// external stack collection signal using sigqueue().
TEST_F(StackTraceCollectorTest, Stress_ExternalSignum_Sigqueue) {
  const int kNumSignallers = 10;
  const int kNumIterations = 5;
  // Step 1: Redirect stderr to a pipe. This helps in verifying the contents
  // of stderr.
  const int stderr_copy = dup(STDERR_FILENO);
  ASSERT_NE(-1, stderr_copy);
  NAMED_DEFER(restore_stderr,
              // Restored stderr at the end of test.
              dup2(stderr_copy, STDERR_FILENO);
              close(stderr_copy););
  int stderr_pipe[2];
  ASSERT_NE(-1, pipe(stderr_pipe));
  DEFER(close(stderr_pipe[0]));
  DEFER(close(stderr_pipe[1]));
  ASSERT_NE(-1, dup2(stderr_pipe[1], STDERR_FILENO));
  close(stderr_pipe[1]);  // stderr_pipe[1] is now an alias for STDERR_FILENO.

  // Step 2: Launch t1 and t2 threads.
  int t1_done[2];  // Used to signal t1 to exit.
  DEFER(close(t1_done[0]));
  DEFER(close(t1_done[1]));
  ASSERT_NE(-1, pipe(t1_done)) << "Errno: " << errno;
  std::promise<void> t2_done;  // Used to signal t2 to exit.
  std::thread t1(F, t2_done.get_future());
  std::thread t2(G, t1_done[0]);

  // Step 3: Launch a task that reads from stderr pipe until the stderr pipe
  // is closed. The task returns the stderr data in @stderr_output future.
  std::future<std::string> stderr_output =
      std::async(std::launch::async, ReadFromFD, stderr_pipe[0]);

  // Step 4: Wait for all threads to start and capture their tids. Then,
  // launch @kNumSignallers child processes, which send @kNumIterations
  // external signal using sigqueue().
  while (5 != common::Sysutil::ListThreads().size()) {
    ;
  }
  const auto tids = common::Sysutil::ListThreads();

  std::set<pid_t> child;
  for (int i = 0; i < kNumSignallers; ++i) {
    auto pid = fork();
    ASSERT_NE(-1, pid) << "Failed to fork";
    // Child process.
    if (pid == 0) {
      RandomGen rng;
      for (int i = 0; i < kNumIterations; ++i) {
        // Send sigqueue to a randomly selected thread.
        while (true) {
          auto tid = tids[rng.NextInt(0, tids.size())];
          union sigval sv;
          if (0 == sigqueue(tid, StackTraceSignal::ExternalSignum(), sv)) {
            break;
          }
        }
      }
      exit(0);
    } else {
      child.insert(pid);
    }
  }

  // Wait for all child processes to finish.
  while (not child.empty()) {
    int status = 0;
    auto pid = waitpid(*child.begin(), &status, 0);
    if (pid > 0) {
      child.erase(pid);
    }
  }
  // Give enough time for all signals to be delivered. Unfortunately there is
  // no easy way to write this test without a sleep, given than signals are
  // delivered out-of-band.
  int sleep_time = 3;
  while (sleep_time > 0) {
    sleep_time = sleep(sleep_time);
  }

  // Step 5: Verify expectations.
  // Redirect stderr back to STDERR_FILNO. Note that this also closes the
  // write end of stderr_pipe, which terminates the @stderr_output task.
  restore_stderr.run_and_expire();

  // Signal t1 and t2 to exit.
  close(t1_done[1]);
  t2_done.set_value();
  t1.join();
  t2.join();

  // Verify that stderr output is as expected.
  auto stderr_str = stderr_output.get();
  auto num_starts = NumMatches(stderr_str, "Stack traces - Start");
  auto num_ends = NumMatches(stderr_str, "Stack traces - End");
  EXPECT_EQ(num_starts, kNumSignallers * kNumIterations) << stderr_str;
  EXPECT_EQ(num_ends, kNumSignallers * kNumIterations) << stderr_str;
  EXPECT_EQ(
      0, NumMatches(stderr_str, "Failed to get all stacktraces within timeout"))
      << stderr_str;
}

// Verifies that external signal handler can safely be installed multiple times.
TEST_F(StackTraceCollectorTest, ExternalHandlerInstalledMultipleTimes) {
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(StackTraceSignal::InstallExternalHandler());
  }
  // Step 1: Redirect stderr to a pipe. This helps in verifying the contents
  // of stderr.
  const int stderr_copy = dup(STDERR_FILENO);
  ASSERT_NE(-1, stderr_copy);
  NAMED_DEFER(restore_stderr,
              // Restored stderr at the end of test.
              dup2(stderr_copy, STDERR_FILENO);
              close(stderr_copy););
  int stderr_pipe[2];
  ASSERT_NE(-1, pipe(stderr_pipe));
  DEFER(close(stderr_pipe[0]));
  DEFER(close(stderr_pipe[1]));
  ASSERT_NE(-1, dup2(stderr_pipe[1], STDERR_FILENO));
  close(stderr_pipe[1]);  // stderr_pipe[1] is now an alias for STDERR_FILENO.

  // Step 2: Launch t1 and t2 threads.
  int t1_done[2];  // Used to signal t1 to exit.
  DEFER(close(t1_done[0]));
  DEFER(close(t1_done[1]));
  ASSERT_NE(-1, pipe(t1_done)) << "Errno: " << errno;
  std::promise<void> t2_done;  // Used to signal t2 to exit.
  std::thread t1(F, t2_done.get_future());
  std::thread t2(G, t1_done[0]);

  // Step 3: Launch a task that reads from stderr pipe until the stderr pipe
  // is closed. The task returns the stderr data in @stderr_output future.
  std::future<std::string> stderr_output =
      std::async(std::launch::async, ReadFromFD, stderr_pipe[0]);

  // Step 4: Wait for all threads to start and capture their tids. Then,
  // launch @kNumSignallers child processes, which send @kNumIterations
  // external signal using sigqueue().
  while (5 != common::Sysutil::ListThreads().size()) {;}
  const auto tids = common::Sysutil::ListThreads();

  auto pid = fork();
  ASSERT_NE(-1, pid) << "Failed to fork";
  // Child process.
  if (pid == 0) {
    auto tid = tids[0];
    kill(tid, StackTraceSignal::ExternalSignum());
    exit(0);
  }

  // Wait for the child process to finish.
  int status = 0;
  while (pid != waitpid(pid, &status, 0)) {;}

  // Give enough time for all signals to be delivered. Unfortunately there is
  // no easy way to write this test without a sleep, given than signals are
  // delivered out-of-band.
  int sleep_time = 2;
  while (sleep_time > 0) {
    sleep_time = sleep(sleep_time);
  }

  // Step 5: Verify expectations.
  // Redirect stderr back to STDERR_FILNO. Note that this also closes the
  // write end of stderr_pipe, which terminates the @stderr_output task.
  restore_stderr.run_and_expire();

  // Signal t1 and t2 to exit.
  close(t1_done[1]);
  t2_done.set_value();
  t1.join();
  t2.join();

  // Verify that stderr output is as expected.
  auto stderr_str = stderr_output.get();
  auto num_starts = NumMatches(stderr_str, "Stack traces - Start");
  auto num_ends = NumMatches(stderr_str, "Stack traces - End");
  EXPECT_EQ(num_starts, num_ends) << stderr_str;
  EXPECT_EQ(1, num_starts) << stderr_str;
  EXPECT_EQ(
      0, NumMatches(stderr_str, "Failed to get all stacktraces within timeout"))
      << stderr_str;
}

// Test the scenario when an external process sends the internal stack
// collection signal. Such signals are ignored.
TEST_F(StackTraceCollectorTest, InternalSignum_FromExternalProcess) {
  // Step 1: Redirect stderr to a pipe. This helps in verifying the contents
  // of stderr.
  const int stderr_copy = dup(STDERR_FILENO);
  ASSERT_NE(-1, stderr_copy);
  NAMED_DEFER(restore_stderr,
              // Restored stderr at the end of test.
              dup2(stderr_copy, STDERR_FILENO);
              close(stderr_copy););
  int stderr_pipe[2];
  ASSERT_NE(-1, pipe(stderr_pipe));
  DEFER(close(stderr_pipe[0]));
  DEFER(close(stderr_pipe[1]));
  ASSERT_NE(-1, dup2(stderr_pipe[1], STDERR_FILENO));
  close(stderr_pipe[1]);  // stderr_pipe[1] is now an alias for STDERR_FILENO.

  // Step 2: Launch a task that reads from stderr pipe until told to exit (via
  // @stderr_done_pipe. The task returns the stderr data in @stderr_output
  // future.
  std::future<std::string> stderr_output =
      std::async(std::launch::async, ReadFromFD, stderr_pipe[0]);
  // Step 3: Fork a child process.
  auto pid = fork();
  ASSERT_NE(-1, pid) << "Fork syscall failed";
  if (pid > 0) {
    // Parent.
    int status;
    auto ret = wait(&status);
    ASSERT_NE(ret, -1) << "Failed to wait for child process to terminate";
    ASSERT_EQ(ret, pid) << "wait(...) returned: " << ret
                        << ", child pid: " << pid;
    // Step 4: Verify expectations.
    // Redirect stderr back to STDERR_FILNO. Note that this also closes the
    // write end of stderr_pipe, which terminates the @stderr_output task.
    restore_stderr.run_and_expire();
    // Verify that stderr output is as expected.
    auto stderr_str = stderr_output.get();
    // EXPECTATION: No stack traces in stderr.
    EXPECT_EQ(0, NumMatches(stderr_str, "Stack traces - Start")) << "STDERR: "
                                                                 << stderr_str;
    EXPECT_EQ(0, NumMatches(stderr_str, "Stack traces - End")) << "STDERR: "
                                                               << stderr_str;
    // EXPECTATION: Message in stderr about ignoring internal signal sent by
    // an external process.
    EXPECT_EQ(
        1,
        NumMatches(stderr_str, "Ignoring signal sent from an outsider pid..."))
        << "STDERR: " << stderr_str;
  } else {
    // Child.
    ASSERT_EQ(0, kill(getppid(), StackTraceSignal::InternalSignum()));
    // Leave enough time to deliver the signal to the parent.
    sleep(1);
    exit(0);
  }
}

// Test the scenario when an external process sends the external stack
// collection signal.
TEST_F(StackTraceCollectorTest, ExternalSignum_FromExternalProcess) {
  // Step 1: Redirect stderr to a pipe. This helps in verifying the contents
  // of stderr.
  const int stderr_copy = dup(STDERR_FILENO);
  ASSERT_NE(-1, stderr_copy);
  NAMED_DEFER(restore_stderr,
              // Restored stderr at the end of test.
              dup2(stderr_copy, STDERR_FILENO);
              close(stderr_copy););
  int stderr_pipe[2];
  ASSERT_NE(-1, pipe(stderr_pipe));
  DEFER(close(stderr_pipe[0]));
  DEFER(close(stderr_pipe[1]));
  ASSERT_NE(-1, dup2(stderr_pipe[1], STDERR_FILENO));
  close(stderr_pipe[1]);  // stderr_pipe[1] is now an alias for STDERR_FILENO.

  // Step 2: Launch a task that reads from stderr pipe until told to exit (via
  // @stderr_done_pipe. The task returns the stderr data in @stderr_output
  // future.
  std::future<std::string> stderr_output =
      std::async(std::launch::async, ReadFromFD, stderr_pipe[0]);
  // Step 3: Fork a child process.
  auto pid = fork();
  ASSERT_NE(-1, pid) << "Fork failed";
  if (pid > 0) {
    // Parent.
    int status;
    auto ret = wait(&status);
    ASSERT_NE(ret, -1) << "Failed to wait for child process to terminate";
    ASSERT_EQ(ret, pid) << "wait(...) returned: " << ret
                        << ", child pid: " << pid;
    // Step 4: Verify expectations.
    // Redirect stderr back to STDERR_FILNO. Note that this also closes the
    // write end of stderr_pipe, which terminates the @stderr_output task.
    restore_stderr.run_and_expire();
    // Verify that stderr output is as expected.
    auto stderr_str = stderr_output.get();
    // EXPECTATION: Exactly one stack trace in stderr.
    EXPECT_EQ(1, NumMatches(stderr_str, "Stack traces - Start")) << "STDERR: "
                                                                 << stderr_str;
    EXPECT_EQ(1, NumMatches(stderr_str, "Stack traces - End")) << "STDERR: "
                                                               << stderr_str;
    // EXPECTATION: No message in stderr about ignoring internal signal sent
    // by an external process.
    EXPECT_EQ(
        0,
        NumMatches(stderr_str, "Ignoring signal sent from an outsider pid..."))
        << "STDERR: " << stderr_str;
  } else {
    // Child.
    ASSERT_EQ(0, kill(getppid(), StackTraceSignal::ExternalSignum()));
    // Leave enough time to deliver the signal to the parent.
    sleep(1);
    exit(0);
  }
}

// Checks for the scenario where the child process receives an external stack
// trace signal. The child process should ignore the signal in this case.
TEST_F(StackTraceCollectorTest, Fork_ChildReceivedStackTraceCollectionSignal) {
  // Step 1: Redirect stderr to a pipe. This helps in verifying the contents
  // of stderr.
  const int stderr_copy = dup(STDERR_FILENO);
  ASSERT_NE(-1, stderr_copy);
  NAMED_DEFER(restore_stderr,
              // Restored stderr at the end of test.
              dup2(stderr_copy, STDERR_FILENO);
              close(stderr_copy););
  int stderr_pipe[2];
  ASSERT_NE(-1, pipe(stderr_pipe));
  DEFER(close(stderr_pipe[0]));
  DEFER(close(stderr_pipe[1]));
  ASSERT_NE(-1, dup2(stderr_pipe[1], STDERR_FILENO));
  close(stderr_pipe[1]);  // stderr_pipe[1] is now an alias for STDERR_FILENO.

  // Step 2: Launch a task that reads from stderr pipe until told to exit (via
  // @stderr_done_pipe. The task returns the stderr data in @stderr_output
  // future.
  std::future<std::string> stderr_output =
      std::async(std::launch::async, ReadFromFD, stderr_pipe[0]);
  // Step 3: Fork a child process.
  auto pid = fork();
  ASSERT_NE(-1, pid) << "Fork failed";
  if (pid > 0) {
    // Parent.
    int status;
    auto ret = wait(&status);
    ASSERT_NE(ret, -1) << "Failed to wait for child process to terminate";
    ASSERT_EQ(ret, pid) << "wait(...) returned: " << ret
                        << ", child pid: " << pid;

    // Step 4: Verify expectations.
    // Redirect stderr back to STDERR_FILNO. Note that this also closes the
    // write end of stderr_pipe, which terminates the @stderr_output task.
    restore_stderr.run_and_expire();
    // Verify that stderr output is as expected.
    auto stderr_str = stderr_output.get();
    // EXPECTATION: No stack traces in stderr.
    EXPECT_EQ(0, NumMatches(stderr_str, "Stack traces - Start")) << "STDERR: "
                                                                 << stderr_str;
    EXPECT_EQ(0, NumMatches(stderr_str, "Stack traces - End")) << "STDERR: "
                                                               << stderr_str;
    // EXPECTATION: Stderr should have message about not contacting stack
    // trace server.
    const std::string expected(
        "Not contacting stack trace server started in a different thread "
        "group");
    EXPECT_EQ(1, NumMatches(stderr_str, expected)) << "STDERR: " << stderr_str;
  } else {
    // Child.
    // Send an external stack trace collection signal to self.
    ASSERT_NE(-1, kill(getpid(), StackTraceSignal::ExternalSignum()));
    // Give enough time for the above signal to be delivered.
    sleep(1);
    exit(0);
  }
}

void Function1(UnbufferedChannel<bool>* in, UnbufferedChannel<bool>* out) {
  bool unused;
  out->Write(true);
  if (in->Read(&unused)) {
    Function1(in, out);
  }
}

// Note that Function2's signature is slightly different from Functions1's
// signature even though they essentially do the same thing. This is done
// deliberately to prevent compiler from deduping the two function. Note that
// the deduping interferes with 'StackTraceCollector.Correctness' test.
bool Function2(UnbufferedChannel<bool>* in,
               UnbufferedChannel<bool>* out,
               bool dummy) {
  bool unused;
  out->Write(true);
  if (in->Read(&unused)) {
    Function2(in, out, dummy);
  }
  return dummy;
}

MATCHER_P3(HasSubStack, tid, function_name, count, "") {
  const std::vector<StackTraceCollector::Result>& got = arg;
  auto it = std::find_if(got.begin(),
                         got.end(),
                         [&](const StackTraceCollector::Result& e) {
                           return std::find_if(e.tids.begin(),
                                               e.tids.end(),
                                               [&](int64_t t) {
                                                 return t == tid;
                                               }) != e.tids.end();
                         });
  if (got.end() == it) {
    *result_listener << "Couldn't find tid: " << tid
                     << " in the results. Results:\n"
                     << StackTraceCollector::ToPrettyString(got);
    return false;
  }
  if (0 == count) {
    return true;
  }

  bool match_started = false;
  int found = 0;
  for (int i = 0; i < it->trace.size(); ++i) {
    const auto& elem = it->trace[i].second;
    if (elem.find(function_name) != std::string::npos) {
      match_started = true;
      if (count == ++found) {
        return true;
      }
    } else if (match_started) {
      break;
    }
  }
  *result_listener << "Found only " << found << " consecutive occurrences of "
                   << function_name << ", expected: " << count << "\nResults:\n"
                   << StackTraceCollector::ToPrettyString(got);
  return false;
}

// This test runs two threads, both of which call two different recursive
// methods. The test thread then controls the depth of recursion of these two
// threads, matching the stack trace expectations with the stack traces reported
// by StackTraceCollector.
TEST_F(StackTraceCollectorTest, Correctness) {
  UnbufferedChannel<bool> in1, in2, out1, out2;
  UnbufferedChannel<pid_t> tid1_ch, tid2_ch;
  auto t1 = std::thread([&] {
    tid1_ch.Write(GetTid());
    Function1(&in1, &out1);
  });
  auto t2 = std::thread([&] {
    tid2_ch.Write(GetTid());
    Function2(&in2, &out2, true /* dummy */);
  });
  DEFER({
    if (t1.joinable()) {
      t1.join();
    }
    if (t2.joinable()) {
      t2.join();
    }
  });
  // Wait for both the threads to start and report their tids.
  pid_t tid1, tid2;
  ASSERT_TRUE(tid1_ch.Read(&tid1));
  ASSERT_TRUE(tid2_ch.Read(&tid2));

  int count1 = 0;
  int count2 = 0;
  // Wait for both threads to call their respective functions.
  {
    bool unused;
    CHECK(out1.Read(&unused));
    CHECK(out2.Read(&unused));
    ++count1;
    ++count2;

    StackTraceCollector collector;
    std::string error;
    auto ret = collector.Collect(&error);
    ASSERT_THAT(error, ::testing::IsEmpty());
    EXPECT_THAT(ret, HasSubStack(tid1, "Function1", 1));
    EXPECT_THAT(ret, HasSubStack(tid2, "Function2", 1));
    // Some negative assertions.
    EXPECT_THAT(ret, ::testing::Not(HasSubStack(tid2, "Function3", 2)));
    EXPECT_THAT(ret, ::testing::Not(HasSubStack(tid1, "Function1", 2)));
    EXPECT_THAT(ret, ::testing::Not(HasSubStack(tid2, "Function2", 2)));
  }

  const auto seed = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
  std::cout << "Seed: " << seed << std::endl;
  RandomGen rng(seed);
  // Note that the current maximum depth of stack traces is 100, and the random
  // numbers are generated while honoring this limit.
  const auto num_iterations = rng.NextInt(5, 10);
  for (int i = 0; i < num_iterations; ++i) {
    auto num1 = rng.NextInt(0, 10);
    auto num2 = rng.NextInt(0, 10);
    for (int i = 0; i < num1; ++i) {
      in1.Write(true);
      // Ensure that the recursive call is already made.
      bool unused;
      CHECK(out1.Read(&unused));
      ++count1;
    }
    for (int i = 0; i < num2; ++i) {
      in2.Write(true);
      // Ensure that the recursive call is already made.
      bool unused;
      CHECK(out2.Read(&unused));
      ++count2;
    }

    StackTraceCollector collector;
    std::string error;
    auto ret = collector.Collect(&error);
    ASSERT_THAT(error, ::testing::IsEmpty());
    EXPECT_THAT(ret, HasSubStack(tid1, "Function1", count1));
    EXPECT_THAT(ret, HasSubStack(tid2, "Function2", count2));
  }
  // Signal @tid1 to quit and verify that its stack trace is no longer reported
  // by the collector.
  {
    in1.Close();
    t1.join();
    // Note that this sleep was added to counter a weird behavior we noticed -
    // Sysutil::ListThreads(), which gets the list of threads from /proc,
    // still returns the tid of the just joined thread, but doesn't return the
    // tid of the next thread (tid2 in this case). Most likely this is because
    // of a race condition between death of @t1 and the reading of /proc by
    // Sysutil::ListThreads(). Adding this small wait fixes this race.
    usleep(100);
    StackTraceCollector collector;
    std::string error;
    auto ret = collector.Collect(&error);
    ASSERT_THAT(error, ::testing::IsEmpty());
    // Verify that there is no stack for @tid1.
    EXPECT_THAT(ret, ::testing::Not(HasSubStack(tid1, "Function1", 0)));
    EXPECT_THAT(ret, HasSubStack(tid2, "Function2", count2));
  }
  // Similarly, signal @tid2 to quit and verify that its stack trace is no
  // longer reported by the collector.
  {
    in2.Close();
    t2.join();
    usleep(100);
    StackTraceCollector collector;
    std::string error;
    auto ret = collector.Collect(&error);
    ASSERT_THAT(error, ::testing::IsEmpty());
    // Verify that there is no stack for @tid1.
    EXPECT_THAT(ret, ::testing::Not(HasSubStack(tid1, "Function1", 0)));
    EXPECT_THAT(ret, ::testing::Not(HasSubStack(tid2, "Function2", 0)));
  }
}

}  // namespace
}  // namespace thoughtspot

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  if (thoughtspot::StackTraceSignal::InstallInternalHandler() &&
      thoughtspot::StackTraceSignal::InstallExternalHandler()) {
    return RUN_ALL_TESTS();
  }
  std::cerr << "Failed to install signal handlers" << std::endl;
  return -1;
}
