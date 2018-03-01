cc_library(
    name = "signal_handler",
    srcs = ["signal_handler.cc"],
    hdrs = ["signal_handler.h"],
    deps = ["//common:channel",
            "//common:defer",
            "//common:sysutil",
            "//common:types",
            "//external:glog", ],
    linkopts = ["-lunwind"],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "signal_handler_test",
    srcs = ["signal_handler_test.cc"],
    deps = [":signal_handler",
            "//common:sysutil",
            "//external:gtest"],
    linkstatic = 1,
)
