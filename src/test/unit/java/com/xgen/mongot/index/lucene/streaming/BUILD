load("//bazel/java:test.bzl", "mongot_java_unit_test_suite")

mongot_java_unit_test_suite(
    name = "lib",
    deps = [
        "//src/main/java/com/xgen/mongot/featureflag",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/knn",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/tracing",
        "//src/main/java/com/xgen/mongot/index/lucene/quantization",
        "//src/main/java/com/xgen/mongot/index/lucene/query/custom",
        "//src/main/java/com/xgen/mongot/index/lucene/streaming",
        "//src/main/java/com/xgen/mongot/index/lucene/vector",
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
)
