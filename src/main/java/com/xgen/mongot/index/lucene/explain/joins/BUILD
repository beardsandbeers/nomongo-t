load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "joins",
    srcs = ["JoinFeatureExplainer.java"],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "//src/main/java/com/xgen/mongot/util/timers",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/tracing",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "//src/main/java/com/xgen/mongot/util/timers",
        "@maven//:com_google_guava_guava",
    ],
)
