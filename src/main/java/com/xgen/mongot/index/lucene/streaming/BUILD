load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "streaming",
    srcs = glob(["*.java"]),
    visibility = ["//:__subpackages__"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/quantization",
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
    ],
)
