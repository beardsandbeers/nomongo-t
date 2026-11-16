load("@mongot_pip//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_library", "py_test")

py_library(
    name = "tools",
    srcs = glob(
        ["*.py"],
        exclude = [
            "check_catalog_workflow_drift.py",
            "test_catalog_workflow_drift.py",
            "test_check_catalog_workflow_drift.py",
        ],
    ),
    srcs_version = "PY3",
    visibility = ["//visibility:public"],
)

py_test(
    name = "check_catalog_workflow_drift_unit_test",
    srcs = [
        "check_catalog_workflow_drift.py",
        "test_check_catalog_workflow_drift.py",
    ],
    main = "test_check_catalog_workflow_drift.py",
    tags = ["lint"],
    deps = [requirement("PyYAML")],
)

py_test(
    name = "catalog_workflow_drift_test",
    srcs = [
        "check_catalog_workflow_drift.py",
        "test_catalog_workflow_drift.py",
    ],
    # The optional filegroup may be empty, which cannot be expanded with $(locations).
    # Tests run from the workspace runfiles root, where the catalog is copied when present.
    args = ["catalog-info.yaml"],
    data = ["//:release_catalog"],
    main = "test_catalog_workflow_drift.py",
    tags = ["lint"],
    deps = [requirement("PyYAML")],
)
