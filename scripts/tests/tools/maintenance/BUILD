load("@rules_python//python:defs.bzl", "py_test")

[py_test(
    name = name,
    srcs = [name + ".py"],
    deps = ["//scripts/tools/maintenance"],
) for name in [
    "test_create_clear_index_request",
    "test_create_force_merge_deletes_request",
    "test_create_request_util",
]]
