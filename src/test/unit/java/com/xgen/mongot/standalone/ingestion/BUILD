load("//bazel/java:test.bzl", "mongot_java_unit_test_suite")

mongot_java_unit_test_suite(
    name = "lib",
    deps = [
        "//src/main/java/com/xgen/mongot/catalog",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/replication/mongodb/common",
        "//src/main/java/com/xgen/mongot/standalone/catalog",
        "//src/main/java/com/xgen/mongot/standalone/config",
        "//src/main/java/com/xgen/mongot/standalone/ingestion",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "//src/proto/standalone/v1:standalone_java_proto",
        "@com_google_protobuf//:protobuf_java",
        "@io_grpc_grpc_java//api",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
    ],
)
