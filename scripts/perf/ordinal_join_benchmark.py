#!/usr/bin/env python3
"""Run isolated ordinal-join kernel comparisons and preserve raw per-operation samples."""

import argparse
import csv
import json
import math
import os
import platform
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = "src/test/bench/java/com/xgen/mongot/index/lucene/query"
TARGET = f"//{PACKAGE}:OrdinalJoinBenchmark"


def scenario(
    kind="objectId",
    foreign=100_000,
    selected=None,
    local=1_000_000,
    local_keys=None,
    foreign_segments=4,
    local_segments=8,
    string_bytes=64,
):
    return {
        "kind": kind,
        "foreign-keys": foreign,
        "selected-keys": foreign if selected is None else selected,
        "local-docs": local,
        "local-keys": foreign if local_keys is None else local_keys,
        "foreign-segments": foreign_segments,
        "local-segments": local_segments,
        "string-bytes": string_bytes,
    }


CASES = {
    "small-objectid": scenario(foreign=10_000, selected=10, local=100_000),
    "objectid-100k": scenario(),
    "objectid-1m": scenario(foreign=1_000_000, local=2_000_000),
    "string64-100k": scenario(kind="string"),
    "string256-100k": scenario(kind="string", string_bytes=256),
    "sparse-objectid": scenario(foreign=1_000_000, selected=10_000, local=2_000_000),
    "many-segments": scenario(foreign_segments=32, local_segments=32),
    "low-overlap": scenario(local_keys=1_000_000),
}


def percentile(values, fraction):
    values = sorted(values)
    return values[math.ceil(len(values) * fraction) - 1]


def summarize(output):
    grouped = {}
    all_rows = []
    for file in sorted(output.glob("*-fork*.csv")):
        case, fork = file.stem.rsplit("-fork", 1)
        with file.open(newline="") as stream:
            for row in csv.DictReader(stream):
                all_rows.append({"case": case, "fork": int(fork), **row})
    if not all_rows and (output / "samples.csv").exists():
        with (output / "samples.csv").open(newline="") as stream:
            all_rows = [{**row, "fork": int(row["fork"])} for row in csv.DictReader(stream)]
    for row in all_rows:
        key = (row["case"], row["shape"], row["strategy"])
        grouped.setdefault(key, []).append(row)
    summary = []
    for (case, shape, strategy), rows in sorted(grouped.items()):
        total = [int(row["total_ns"]) / 1e6 for row in rows]
        preparation = [int(row["preparation_ns"]) / 1e6 for row in rows]
        allocated = [int(row["allocated_bytes"]) for row in rows]
        fork_medians = {
            str(fork): statistics.median(
                int(row["total_ns"]) / 1e6 for row in rows if row["fork"] == fork
            )
            for fork in sorted({row["fork"] for row in rows})
        }
        summary.append(
            {
                "case": case,
                "shape": shape,
                "strategy": strategy,
                "samples": len(rows),
                "p50_ms": statistics.median(total),
                "p95_ms": percentile(total, 0.95),
                "preparation_p50_ms": statistics.median(preparation),
                "allocated_p50_bytes": statistics.median(allocated),
                "retained_query_bytes": int(rows[0]["retained_query_bytes"]),
                "peak_membership_bytes": int(rows[0]["peak_membership_bytes"]),
                "expected_hits": int(rows[0]["expected_hits"]),
                "fork_p50_ms": fork_medians,
            }
        )
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    if summary:
        with (output / "samples.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(all_rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(all_rows)
        fields = [field for field in summary[0] if field != "fork_p50_ms"]
        with (output / "summary.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(
                stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(summary)
    return summary


def write_chart(summary, destination):
    template = Path(__file__).with_name("ordinal_join_benchmark_chart.html").read_text()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(template.replace("__SUMMARY_JSON__", json.dumps(summary)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--case", action="append", choices=CASES)
    parser.add_argument(
        "--shape", action="append", choices=["selectiveTop10", "broadTop10", "broadCount"]
    )
    parser.add_argument("--forks", type=int, default=2)
    parser.add_argument("--warmups", type=int, default=15)
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--scan-before-seek", type=int, default=0)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--summarize-only", action="store_true")
    parser.add_argument("--html-output", type=Path)
    args = parser.parse_args()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if args.summarize_only:
        summary = summarize(output)
        if args.html_output:
            write_chart(summary, args.html_output)
        return
    if not os.path.ismount("/hdd"):
        raise RuntimeError("/hdd is not mounted; cannot create or use persistent build caches")
    cases = args.case or list(CASES)
    existing = list(output.glob("*-fork*.csv"))
    if existing:
        raise FileExistsError(f"Choose a fresh output directory; existing samples: {existing[0]}")
    cache = Path("/hdd/michael/dev-cache")
    output_base = cache / "mongot/worktrees" / ROOT.name / "bazel-output"
    bazel = ["bazel", f"--output_base={output_base}"]
    if not args.skip_build:
        subprocess.run(
            [
                *bazel,
                "build",
                f"--repository_cache={cache / 'bazel-repository'}",
                "--lockfile_mode=off",
                "--jobs=4",
                TARGET,
            ],
            cwd=ROOT,
            check=True,
        )
        # Release compiler workers before timing and avoid their retained heap competing with tests.
        subprocess.run([*bazel, "shutdown"], cwd=ROOT, check=True)
    metadata = {
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "dirty": bool(
            subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
        ),
        "forks": args.forks,
        "warmups_per_shape_and_strategy": args.warmups,
        "samples_per_shape_and_strategy_and_fork": args.samples,
        "scan_before_seek": args.scan_before_seek,
        "shapes": args.shape or ["selectiveTop10", "broadTop10", "broadCount"],
        "cases": {name: CASES[name] for name in cases},
        "cache": "query caches disabled; warm FSDirectory readers; no transport or production admission",
        "memory": {
            "allocated": "ThreadMXBean thread allocation delta over preparation plus search",
            "retained": "Lucene RamUsageTester graph size of prepared membership query; excludes readers",
            "peak_membership": "owned membership structures during preparation; hash+current seen set sampled at leaf boundaries/every 4096 documents, or sum of foreign/local ordinal bitmaps; excludes iterators, reallocation overlap, and query rewrite; not JVM heap high-water mark",
        },
    }
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    binary = (ROOT / "bazel-bin" / PACKAGE / "OrdinalJoinBenchmark").resolve()
    started = time.monotonic()
    for fork in range(args.forks):
        for case in cases:
            print(f"Starting {case}, fork {fork + 1}/{args.forks}", flush=True)
            result = output / f"{case}-fork{fork}.csv"
            if result.exists():
                raise FileExistsError(f"Refusing to overwrite raw samples: {result}")
            with tempfile.TemporaryDirectory(prefix="ordinal-join-index-", dir="/tmp") as index:
                command = [str(binary), "--index-directory", index, "--output", str(result)]
                options = {
                    **CASES[case],
                    "warmups": args.warmups,
                    "samples": args.samples,
                    "seed": 42 + fork,
                    "scan-before-seek": args.scan_before_seek,
                    "shapes": ",".join(
                        args.shape or ["selectiveTop10", "broadTop10", "broadCount"]
                    ),
                }
                for key, value in options.items():
                    command.extend([f"--{key}", str(value)])
                with (output / f"{case}-fork{fork}.log").open("w") as log:
                    subprocess.run(
                        command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True
                    )
            print((output / f"{case}-fork{fork}.log").read_text(), flush=True)
            summarize(output)
    metadata["elapsed_seconds"] = time.monotonic() - started
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    if args.html_output:
        write_chart(summarize(output), args.html_output)
    print(f"Results: {output / 'summary.json'}", flush=True)


if __name__ == "__main__":
    main()
