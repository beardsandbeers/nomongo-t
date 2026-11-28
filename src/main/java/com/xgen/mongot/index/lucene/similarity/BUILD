load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "similarity",
    srcs = [
        "LuceneSimilarity.java",
        "SimilarityWrapper.java",
        "StableTflSimilarity.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:org_apache_lucene_lucene_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer/wrapper",
        "//src/main/java/com/xgen/mongot/index/definition",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_flogger_flogger",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
