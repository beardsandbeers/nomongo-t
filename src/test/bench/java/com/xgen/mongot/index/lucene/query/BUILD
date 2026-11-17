load("@rules_java//java:java_binary.bzl", "java_binary")
load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "ordinal_join_prototype",
    testonly = True,
    srcs = ["OrdinalJoinFixture.java"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/query",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)

java_library(
    name = "benchmark",
    testonly = True,
    srcs = ["OrdinalJoinBenchmark.java"],
    deps = [
        ":ordinal_join_prototype",
        "//src/main/java/com/xgen/mongot/index/lucene/query",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "@maven//:org_apache_lucene_lucene_analysis_common",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_test_framework",
    ],
)

java_binary(
    name = "OrdinalJoinBenchmark",
    testonly = True,
    jvm_flags = [
        "-Xms768m",
        "-Xmx1536m",
        "--add-modules=jdk.incubator.vector",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--enable-native-access=ALL-UNNAMED",
    ],
    main_class = "com.xgen.mongot.index.lucene.query.OrdinalJoinBenchmark",
    runtime_deps = [":benchmark"],
)
