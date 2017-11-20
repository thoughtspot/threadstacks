cc_library(
    name = "channel",
    hdrs = ["channel.h",
            "unbuffered_channel.h", ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "unbuffered_channel_test",
    srcs = ["unbuffered_channel_test.cc"],
    deps = [":channel",
            ":defer",
            "//external:glog",
            "//external:gtest_main",],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "defer",
    hdrs = ["defer.h"],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "defer_test",
    srcs = ["defer_test.cc"],
    deps = [":defer",
            "//external:gtest_main",],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "sysutil",
    hdrs = ["sysutil.h"],
    srcs = ["sysutil.cc"],
    deps = [":defer"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "types",
    hdrs = ["types.h"],
    visibility = ["//visibility:public"],
)
