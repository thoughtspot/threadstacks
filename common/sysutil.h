// Copyright: ThoughtSpot Inc 2017
// Author: Nipun Sehrawat (nipun@thoughtspot.com)

#ifndef COMMON_SYSUTIL_H_
#define COMMON_SYSUTIL_H_

#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace thoughtspot {
namespace common {

class Sysutil {
 public:
  // Returns a list of thread pids that are running in the calling process. On
  // error, returns an empty list.
  static std::vector<pid_t> ListThreads();
};

}  // namespace common
}  // namespace thoughtspot

#endif  // COMMON_SYSUTIL_H_
