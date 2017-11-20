new_git_repository(
    name = "googletest",
    build_file = "googletest.BUILD",
    remote = "https://github.com/google/googletest",
    tag = "release-1.8.0",
)

bind(
    name = "gtest",
    actual = "@googletest//:gtest",
)

bind(
    name = "gtest_main",
    actual = "@googletest//:gtest_main",
)

git_repository(
    name   = "com_github_gflags_gflags",
    remote = "https://github.com/gflags/gflags.git",
    tag = "v2.2.1",
)

new_git_repository(
    name   = "com_github_glog_glog",
    build_file = "glog.BUILD",
    remote = "https://github.com/google/glog.git",
    tag = "v0.3.5",
)

bind(
    name = "glog",
    actual = "@com_github_glog_glog//:glog",
)
