load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "bootstrap",
    srcs = ["Mongot.java"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/community",
        "//src/main/java/com/xgen/mongot/config/provider/cli",
        "//src/main/java/com/xgen/mongot/config/provider/community",
        "//src/main/java/com/xgen/mongot/standalone",
        "//src/main/java/com/xgen/mongot/standalone/config",
        "@maven//:com_google_guava_guava",
        "@maven//:info_picocli_picocli",
    ],
)
