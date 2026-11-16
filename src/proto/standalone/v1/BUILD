load("@com_google_protobuf//bazel:java_proto_library.bzl", "java_proto_library")
load("@io_grpc_grpc_java//:java_grpc_library.bzl", "java_grpc_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/java:package.bzl", "java_library")

exports_files(["standalone.proto"])

proto_library(
    name = "standalone_proto",
    srcs = ["standalone.proto"],
    visibility = ["//visibility:public"],
)

java_proto_library(
    name = "standalone_java_proto",
    visibility = ["//visibility:public"],
    deps = [":standalone_proto"],
)

java_grpc_library(
    name = "standalone_java_grpc",
    srcs = [":standalone_proto"],
    visibility = ["//visibility:public"],
    deps = [":standalone_java_proto"],
)

# Messages and service stubs share one Java package. Gazelle resolves packages to one label.
java_library(
    name = "standalone_java",
    visibility = ["//visibility:public"],
    exports = [
        ":standalone_java_grpc",
        ":standalone_java_proto",
    ],
)
