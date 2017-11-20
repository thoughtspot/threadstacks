// Copyright: ThoughtSpot Inc 2017
// Author: Nipun Sehrawat (nipun@thoughtspot.com)

#ifndef THREADSTACKS_SIGNAL_HANDLER_H_
#define THREADSTACKS_SIGNAL_HANDLER_H_

#include <signal.h>
#include <sys/types.h>

#include <string>
#include <vector>

#include "common/types.h"

namespace thoughtspot {

// A StackTraceCollector can be used for collecting stack traces of all threads
// running in the current process.
class StackTraceCollector {
 public:
  // Result of the stack trace collection process.
  struct Result {
    // Stacktrace as a collection of (address, symbol) pairs. The first element
    // in the vector is the top of the stacktrace.
    std::vector<std::pair<int64_t, std::string>> trace;
    // List of tids that share the above stack trace.
    std::vector<pid_t> tids;
  };

  // Returns a pretty string containing all the stack traces in @result.
  static std::string ToPrettyString(const std::vector<Result>& result);

  StackTraceCollector() = default;
  ~StackTraceCollector() = default;

  // Returns stack traces of all threads in the system. Returns an empty vector
  // on encountering an error, in which case @error is filled with a descriptive
  // error message.
  std::vector<Result> Collect(std::string* error);
};

// StackTraceSignal class provides some utility methods to install internal and
// external stacktrace collection signal handlers.
class StackTraceSignal {
 public:
  // Returns the signal number used for the internal stack trace collection
  // mechanism.
  static int InternalSignum();
  // Returns the signal number that can be used to trigger stacktrace
  // collection in this process.
  static int ExternalSignum();

  // Installs the internal stacktrace collection signal handler.
  static bool InstallInternalHandler();
  // Installs the external stacktrace collection signal handler.
  static bool InstallExternalHandler();

  // TODO(nipun): Expose an async-signal-safe function to request dumping
  // of stack traces to stderr. Such a function can be called from signal
  // handlers of fatal signals such as SIGABRT, SIGSEGV, SIGTERM, etc. to get
  // traces of all threads upon receiving a fatal signal.
};

}  // namespace thoughtspot

#endif  // THREADSTACKS_SIGNAL_HANDLER_H_
