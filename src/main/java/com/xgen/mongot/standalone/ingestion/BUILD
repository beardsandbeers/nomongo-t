load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "ingestion",
    srcs = [
        "ApiDocumentIndexer.java",
        "ApiIngestionManager.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/util/mongodb",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/catalog",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/replication",
        "//src/main/java/com/xgen/mongot/replication/mongodb/common",
        "//src/main/java/com/xgen/mongot/standalone:bson",
        "//src/main/java/com/xgen/mongot/standalone/config",
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "//src/main/java/com/xgen/mongot/util/regex",
        "//src/proto/standalone/v1:standalone_java",
        "@com_google_protobuf//:protobuf_java",
        "@io_grpc_grpc_java//api",
        "@maven//:org_mongodb_bson",
    ],
)
