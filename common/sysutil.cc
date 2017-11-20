// Copyright: ThoughtSpot Inc 2017
// Author: Nipun Sehrawat (nipun@thoughtspot.com)

#include "common/sysutil.h"

#include <dirent.h>
#include <iostream>
#include <set>
#include <string>
#include "common/defer.h"

namespace thoughtspot {
namespace common {
namespace {
const char* kSelfTaskDir = "/proc/self/task";

bool GetDirectoryContents(const std::string& directory,
                          std::set<std::string>* children,
                          std::string* error) {
  children->clear();
  DIR* dir = opendir(directory.c_str());
  if (dir == nullptr) {
    error->assign("Failed to open directory: " + directory);
    return false;
  }
  DEFER(closedir(dir));
  struct dirent entry;
  struct dirent* result = nullptr;
  int posix_error = 0;
  while (true) {
    posix_error = readdir_r(dir, &entry, &result);
    if (posix_error != 0 || result == nullptr) {
      break;
    }
    const std::string child(entry.d_name);
    if (child == "." || child == "..") {
      continue;
    }
    children->insert(child);
  }
  if (posix_error != 0) {
    error->assign("Error reading directory: " + std::to_string(posix_error));
    children->clear();
    return false;
  }
  return true;
}

}  // namespace

// static
std::vector<pid_t> Sysutil::ListThreads() {
  std::set<std::string> children;
  std::vector<pid_t> pids;
  std::string error;
  if (not GetDirectoryContents(kSelfTaskDir, &children, &error)) {
    std::cerr << "Unable to list threads in current process. Error: " << error
              << std::endl;
    return pids;
  }
  for (const auto& child : children) {
    try {
      pids.push_back(stoll(child));
    } catch (const std::exception& ex) {
      continue;
    }
  }
  return pids;
}

}  // namespace common
}  // namespace thoughtspot
