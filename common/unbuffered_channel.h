// Copyright: ThoughtSpot Inc 2017
// Author: Nipun Sehrawat (nipun@thoughtspot.com)
// Author: Abhishek Rai (abhishek@thoughtspot.com)

#ifndef COMMON_UNBUFFERED_CHANNEL_H_
#define COMMON_UNBUFFERED_CHANNEL_H_

#include <memory>
#include <condition_variable>
#include <mutex>

#include "common/channel.h"
#include "common/defer.h"
#include "glog/logging.h"

// An UnbufferedChannel is used to facilitate a handshake between a writer and a
// reader. A writer blocks until there is a reader ready for the handshake.
// Similarly, a reader of an UnbufferedChannel blocks until there is a writer
// ready for the handshake. An UnbufferedChannel can be thought of as a buffered
// channel with 0 capacity.
//
// Algorithm for Write() (sans timeouts and channel closing):
// 1. Wait until there is no writer is handshake zone already.
// 2. Enter the handshake zone and mark that a writer is in handshake zone.
// 3. Write the value.
// 4. Notify one reader to enter handshake zone.
// 5. Wait for reader to enter the zone and consume the value.
// 6. Mark that no writer and no reader (reader is guaranteed to have left the
//    zone) is in handshake zone.
// 7. Notify one of the existing writers to enter the zone.
//
// Algorithm for Read() (sans timeouts and channel closing):
// 1. Wait until there is one writer, but no reader, is in handshake zone.
// 2. Enter the handshake zone and mark that a reader is in handshake zone.
// 3. Read the value.
// 4. Notify the writer (which is guaranteed to be waiting for the reader) that
//    reader entered the zone and consumed the value.
namespace thoughtspot {
namespace common {
template <typename ValueType>
class UnbufferedChannel : public Channel<ValueType> {
 public:
  UnbufferedChannel() = default;
  ~UnbufferedChannel() = default;

  void Write(const ValueType& item) override {
    bool timedout = false;
    Write(item, kInifinity, &timedout);
  }
  void Write(const ValueType& item,
             int64_t wait_duration,
             bool* timedout) override {
    std::unique_lock<std::mutex> l(m_);
    // Wait if there is already a writer in the handshake zone.
    auto success =
        writers_.wait_for(l,
                          std::chrono::microseconds(wait_duration),
                          [this]() { return closed_ || not writter_in_zone_; });
    if (not success) {
      *timedout = true;
      return;
    }
    LOG_IF(FATAL, closed_) << "Can't write to a closed channel";

    /////////////////////////// BEGIN: Handshake zone /////////////////////////

    // Copy data into @item_ and signal exactly one reader to proceed with the
    // handshake.
    writter_in_zone_ = true;
    item_ = &item;
    readers_.notify_one();

    // Wait for a reader to enter (and exit) the handshake zone.
    success =
        handshake_.wait_for(l,
                            std::chrono::microseconds(wait_duration),
                            [this]() { return closed_ || reader_in_zone_; });
    // No reader could handshake within the timeout, so timeout this write.
    if (not success) {
      *timedout = true;
      writter_in_zone_ = false;
      writers_.notify_one();
      return;
    }
    // If the reader hasn't consumed the value being produced by this writer,
    // and the channel was closed, then we've got a case of concurrent write()
    // and close(), which calls for a LOG(FATAL).
    //
    // Note that the following sequence is valid and doesn't warrant a
    // LOG(FATAL):
    //
    // Thread 1: write()
    // Thread 2: read()
    // Thread 2: close() - At this point, Thread 1's write() might have come out
    //                     of handshake_.wait(), but still waiting for acquire
    //                     mutex @m_. Meanwhile close() can acquire @m_ and
    //                     update closed_ to false. As the value produced by
    //                     write() has already been consumed by read(), there is
    //                     no need to LOG(FATAL) if the channel has been closed.
    LOG_IF(FATAL, not reader_in_zone_ && closed_)
        << "Can't write to a closed channel";
    *timedout = false;

    // Signal exactly one pending writer to proceed.
    writter_in_zone_ = false;
    // At this time we are sure that the reader has exited the zone.
    reader_in_zone_ = false;
    writers_.notify_one();
    /////////////////////////// END: Handshake zone /////////////////////////
  }

  bool Read(ValueType* item) override {
    bool timedout = false;
    return Read(item, kInifinity, &timedout);
  }
  bool Read(ValueType* item, int64_t wait_duration, bool* timedout) override {
    std::unique_lock<std::mutex> l(m_);
    // Wait if a reader is already in the handshake zone or if there is no
    // writer in the handshake zone.
    auto success = readers_.wait_for(
        l,
        std::chrono::microseconds(wait_duration),
        [this]() {
          return closed_ || (not reader_in_zone_ && writter_in_zone_);
        });
    if (not success) {
      *timedout = true;
      DCHECK(not closed_);
      return false;
    }
    /////////////////////////// BEGIN: Handshake zone /////////////////////////
    *timedout = false;
    if (closed_) {
      *item = ValueType();
      return false;
    }
    // Signal the blocked writer to proceed, as the value has been consumed.
    DCHECK(writter_in_zone_);
    reader_in_zone_ = true;
    *item = *item_;
    handshake_.notify_one();
    /////////////////////////// END: Handshake zone /////////////////////////
    return true;
  }

  void Close() override {
    {
      std::lock_guard<std::mutex> l(m_);
      LOG_IF(FATAL, closed_) << "Can't close an already closed channel";
      closed_ = true;
    }
    writers_.notify_all();
    readers_.notify_all();
    // There can be at most one writer waiting for handshake to finish.
    handshake_.notify_one();
  }

 private:
  // For some reason, using very large wait times in wait_for(...) returns a
  // timeout immediately. Perhaps the implementation of wait_for(...)
  // concludes that very large waits (few hundreds of centuaries) will always
  // timeout, so it returns with a timeout immediately.
  static constexpr int64_t kInifinity{365LL * 24 * 3600 * 1000000LL};

  std::mutex m_;
  // True if the channel has been closed.
  bool closed_ = false;
  // True if a writer has entered the handshake zone.
  // INVARIANT 1: At any given time, at most one writer can be in the handshake
  //              zone.
  bool writter_in_zone_ = false;
  // True if a reader has entered the handshake zone.
  // INVARIANT 1: A reader can enter the handshake zone only after a
  //              corresponding writer has entered the zone and signalled this
  //              reader to enter the zone.
  // INVARIANT 2: At any given time, at most one reader can be in the handshake
  //              zone.
  bool reader_in_zone_ = false;
  // Used by writers to enter the handshake zone.
  std::condition_variable writers_;
  // Used by readers to enter the handshake zone.
  std::condition_variable readers_;
  // Used by a reader in handshake zone to notify the corresponding writer in
  // the zone about a successful handshake.
  std::condition_variable handshake_;

  // Used to exchange the value being communicated during a reader-writer
  // handshake. Note the pointer remains valid only for a reader the the
  // handshake zone (because the writer who set this value is also guaranteed to
  // be in the zone, waiting for the reader to finish).
  const ValueType* item_ = nullptr;

  // Disable copy c'tor and assignment operator.
  UnbufferedChannel(const UnbufferedChannel&) = delete;
  UnbufferedChannel& operator=(const UnbufferedChannel&) = delete;
};

// static
template <typename T>
constexpr int64_t UnbufferedChannel<T>::kInifinity;

}  // namespace common
}  // namespace thoughtspot

#endif  // COMMON_UNBUFFERED_CHANNEL_H_
