load("@rules_java//java:defs.bzl", "java_binary")

# gazelle:resolve java com.xgen.mongot.standalone //src/main/java/com/xgen/mongot/standalone

load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "bootstrap",
    srcs = ["Mongot.java"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/community",
        "//src/main/java/com/xgen/mongot/standalone",
        "//src/main/java/com/xgen/mongot/standalone/config",
        "@maven//:com_google_guava_guava",
        "@maven//:info_picocli_picocli",
    ],
)

java_binary(
    name = "Mongot",
    main_class = "com.xgen.mongot.bootstrap.Mongot",
    visibility = ["//visibility:public"],
    runtime_deps = [":bootstrap"],
)
