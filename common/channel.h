// Copyright: ThoughtSpot Inc 2017
// Author: Nipun Sehrawat (nipun@thoughtspot.com)

#ifndef COMMON_CHANNEL_H_
#define COMMON_CHANNEL_H_


namespace thoughtspot {
namespace common {

// Channel is a communication primitive for building concurrent applications. It
// can be used to communicate values between concurrently executing entities, in
// a thread-safe manner.
//
// The Channel interface is a super-set of Golang's channel interface. Read(),
// Write() and Close() functions behave the same as in Golang, but a timed
// variant of Read() and Write() is also made available. The timed variants are
// useful to approximate Golang's select statement.
//
// Note: Implementations of this class are expected to be thread-safe.
template <typename T>
class Channel {
 public:
  using ValueType = T;
  Channel() = default;
  virtual ~Channel() = default;

  // Writes @item to the channel.
  // Blocks if the channel is open and full to its capacity.
  virtual void Write(const ValueType& item) = 0;
  // Writes @item to the channel, subject to timeout of @wait_duration
  // microseconds. If the values can't be written (because the channel is full
  // to its capacity) within @wait_duration, @timedout is set to true.
  // Blocks if the channel is full to its capacity.
  virtual void Write(const ValueType& item,
                     int64_t wait_duration,
                     bool* timedout) = 0;
  // Reads the next value in channel and populates it in @item. Returns true if
  // the value was previously written to the channel by a successful Write(...).
  // Else, returns false - in this case the channel has been closed and all
  // the pending values have already been read. Read(...) returns immediately in
  // this case, and @item is populated with the default value of ValueType.
  // Blocks if the channel is open and empty.
  virtual bool Read(ValueType* item) = 0;
  // Reads the next value in channel and populates @item with it, subject to
  // deadline of @wait_duration microseconds. If the value can't be read
  // (because the channel is empty) within @wait_duration, @timedout is set to
  // true.
  //
  // Returns true if the value was previously written to the channel by a
  // successful Write(...).
  //
  // Returns false in the following two cases:
  // 1. The read timed out.
  // 2. The channel has been closed and all the channel values have already been
  //    read. In this case, Read(...) returns immediately, and @item is
  //    populated with the default value of ValueType.
  // Blocks if the channel is open and empty.
  virtual bool Read(ValueType* item, int64_t wait_duration, bool* timedout) = 0;
  // Closes a channel for any further writes. Already written values are
  // avaiable to reading, even after the channel has been closed. Any extra
  // reads return the default value of ValueType. Unblocks all readers.
  //
  // Note that the following two behavior will result in a RUNTIME CRASH:
  // 1. Closing an already closed channel.
  // 2. Writing to a closed channel.
  //
  // A call to Close() never blocks.
  virtual void Close() = 0;
};

}  // namespace common
}  // namespace thoughtspot

#endif  // COMMON_CHANNEL_H_
