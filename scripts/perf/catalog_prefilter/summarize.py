"""Check the measured acceptance against the source metadata and summarize 500 ms crossings."""

import argparse
import csv
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path


def reference_counts(path):
    counts = Counter()
    with path.open() as f:
        for _, p, r, n, category in csv.reader(f, delimiter="\t"):
            price = bool(p) and 5 <= float(p) <= 100
            rating = bool(r) and 4 <= float(r) <= 5
            reviews = bool(n) and float(n) >= 10
            except_books = category != "Books"
            for name, matches in (
                ("price_5_to_100", price),
                ("rating_at_least_4", rating),
                ("reviews_at_least_10", reviews),
                ("except_books", except_books),
                ("price_and_rating", price and rating),
                ("price_rating_reviews", price and rating and reviews),
                (
                    "price_rating_reviews_except_books",
                    price and rating and reviews and except_books,
                ),
            ):
                counts[name] += matches
    return counts


def summarize(cohorts, results):
    rows = []
    for leaf in ("0", "1"):
        counts = reference_counts(cohorts / leaf / "products.tsv")
        groups = defaultdict(list)
        with (results / f"leaf{leaf}.csv").open() as f:
            for row in csv.DictReader(f):
                name, mode = row["filter"], row["cache"]
                assert int(row["accepted"]) == counts[name]
                groups[name, mode].append((int(row["round"]), int(row["materialization_ns"]) / 1e6))
        for name in counts:
            for mode in ("eviction_requested", "warm"):
                samples = groups.pop((name, mode))
                assert sorted(i for i, _ in samples) == list(range(25))
                times = sorted(ms for _, ms in samples)
                rows.append(
                    [
                        leaf,
                        name,
                        mode,
                        counts[name],
                        len(times),
                        statistics.median(times),
                        times[math.ceil(0.95 * len(times)) - 1],
                        times[-1],
                        sum(ms >= 500 for ms in times),
                    ]
                )
        assert not groups
    with (results / "summary.csv").open("w") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(
            [
                "leaf",
                "filter",
                "cache",
                "accepted",
                "samples",
                "p50_ms",
                "p95_ms",
                "max_ms",
                "samples_ge_500ms",
            ]
        )
        w.writerows(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cohorts", type=Path)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()
    summarize(args.cohorts, args.results)
