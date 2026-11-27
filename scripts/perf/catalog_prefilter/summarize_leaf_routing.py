"""Summarize the per-segment search sweep: python3 summarize_leaf_routing.py trials.zip > summary.csv."""

import csv
import io
import random
import statistics
import sys
import zipfile
from collections import defaultdict


def summarize(archive, writer):
    for filename in sorted(archive.namelist()):
        if not filename.endswith(".csv"):
            continue
        experiment = filename.split("/")[0]
        if experiment == "materialization":
            continue
        trials = defaultdict(lambda: defaultdict(list))
        with archive.open(filename) as contents:
            for row in csv.DictReader(io.TextIOWrapper(contents)):
                key = tuple(
                    row[k] for k in ("codec", "vectors", "filter", "matches", "limit", "candidates")
                )
                trials[key][row["mode"], int(row["query"])].append(row)
        for key, modes in sorted(trials.items()):
            query_ids = sorted(q for mode, q in modes if mode == "classic")
            for mode in sorted({mode for mode, _ in modes} - {"classic"}):
                timings = defaultdict(list)
                recalls = defaultdict(list)
                stream_counts = set()
                for q in query_ids:
                    for label in ("classic", mode):
                        reps = modes[label, q]
                        assert sorted(int(row["rep"]) for row in reps) == [0, 1]
                        timings[label].append(statistics.median(float(row["ms"]) for row in reps))
                        recalls[label].append(
                            statistics.mean(
                                100 * int(row["recall_hits"]) / int(key[4]) for row in reps
                            )
                        )
                    if mode == "auto":
                        stream_counts.update(row["stream_segments"] for row in modes[mode, q])
                if mode == "auto":
                    assert len(stream_counts) == 1
                # Pair on query. Repetitions of a deterministic graph walk aren't independent recall samples.
                delta = [m - c for m, c in zip(recalls[mode], recalls["classic"])]
                rng = random.Random(944)
                bootstrap = sorted(
                    sum(rng.choices(delta, k=len(delta))) / len(delta) for _ in range(2000)
                )
                writer.writerow(
                    [
                        experiment,
                        *key,
                        mode,
                        next(iter(stream_counts), ""),
                        len(query_ids),
                        *[
                            round(x, 4)
                            for label in ("classic", mode)
                            for x in (
                                statistics.median(timings[label]),
                                statistics.quantiles(timings[label], n=100, method="inclusive")[94],
                                statistics.mean(recalls[label]),
                            )
                        ],
                        round(statistics.mean(delta), 4),
                        round(bootstrap[49], 4),
                        round(bootstrap[1949], 4),
                    ]
                )


if __name__ == "__main__":
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(
        "experiment codec vectors filter matches limit candidates mode stream_segments queries classic_p50_ms classic_p95_ms classic_recall_pct mode_p50_ms mode_p95_ms mode_recall_pct recall_delta_pp delta_low95_pp delta_high95_pp".split()
    )
    with zipfile.ZipFile(sys.argv[1]) as archive:
        summarize(archive, writer)
