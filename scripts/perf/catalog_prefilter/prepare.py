"""Read verified Superlinked catalog shards and preserve each product's vector and filter fields."""

import argparse
import csv
import hashlib
import json
import shutil
import urllib.request
from itertools import islice
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

REV = "9baf92ae5fb8f4746ed227fa6c88e66c7783fa23"
REPO = "superlinked/external-benchmarking"
ROWS = 312_000
INT8_ROWS = 1_130_000
INT8_SCALE = 256
DIMS = 4154


def shard_path(cache, entry):
    relative = entry["path"]
    path = cache / Path(relative).name
    if not path.exists():
        url = f"https://huggingface.co/datasets/{REPO}/resolve/{REV}/{relative}?download=true"
        with (
            urllib.request.urlopen(url, timeout=120) as src,
            path.with_suffix(".tmp").open("wb") as out,
        ):
            shutil.copyfileobj(src, out)
        path.with_suffix(".tmp").rename(path)
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    if digest.hexdigest() != entry["lfs"]["oid"]:
        raise ValueError(f"Checksum mismatch on {path}")
    return path


def products(cache, int8):
    api = f"https://huggingface.co/api/datasets/{REPO}/tree/{REV}/benchmark-10M?limit=1000"
    with urllib.request.urlopen(api, timeout=60) as response:
        entries = json.load(response)
    seen = set()
    fields = ["parent_asin", "price", "average_rating", "rating_number", "main_category", "value"]
    for entry in sorted(entries, key=lambda e: e["path"]):
        if not entry["path"].endswith(".parquet"):
            continue
        path = shard_path(cache, entry)
        for batch in pq.ParquetFile(path).iter_batches(batch_size=256, columns=fields):
            vectors = batch.column("value")
            if vectors.null_count or not np.all(np.diff(vectors.offsets) == DIMS):
                raise ValueError(f"Invalid embedding in {path}")
            floats = vectors.flatten().to_numpy().reshape(-1, DIMS).astype("<f4")
            if not np.isfinite(floats).all():
                raise ValueError(f"Nonfinite embedding in {path}")
            if int8:
                codes = np.rint(floats * INT8_SCALE)
                if codes.min() < -127 or codes.max() > 127:
                    raise ValueError(f"Embedding exceeds the fixed int8 range in {path}")
                floats = codes.astype(np.int8)
            products = zip(*(batch.column(f).to_pylist() for f in fields[:-1]))
            for i, product in enumerate(products):
                if not product[0] or product[0] in seen:
                    continue
                seen.add(product[0])
                yield product, floats[i]


def prepare(cache, target, int8=False):
    cache.mkdir(parents=True, exist_ok=True)
    target.mkdir(parents=True, exist_ok=False)
    records = products(cache, int8)
    rows = INT8_ROWS if int8 else ROWS
    vector_file = "vectors.i8" if int8 else "vectors.f32"
    for cohort in ("0", "1"):
        folder = target / cohort
        folder.mkdir()
        with (
            (folder / vector_file).open("xb") as data,
            (folder / "products.tsv").open("x", newline="") as metadata,
        ):
            writer = csv.writer(
                metadata, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_NONE
            )
            count = 0
            for product, vector in islice(records, rows):
                data.write(vector.tobytes())
                writer.writerow(product)
                count += 1
            if count != rows:
                raise ValueError("Insufficient unique products")


def prepare_queries(cohorts, shard, output, count=16):
    """Select distinct, non-indexed queries from a pinned held-out catalog shard."""
    if count < 1:
        raise ValueError("Query count must be positive")
    indexed = set()
    for cohort in ("0", "1"):
        with (cohorts / cohort / "products.tsv").open() as metadata:
            indexed.update(line.partition("\t")[0] for line in metadata)
    vectors = []
    for batch in pq.ParquetFile(shard).iter_batches(
        batch_size=128, columns=["parent_asin", "value"]
    ):
        for row in range(0, batch.num_rows, 13):
            product = batch.column("parent_asin")[row].as_py()
            vector = batch.column("value")[row]
            if not product or product in indexed:
                continue
            codes = np.rint(np.asarray(vector.as_py(), dtype=np.float32) * INT8_SCALE)
            if (
                codes.shape != (DIMS,)
                or not np.isfinite(codes).all()
                or codes.min() < -127
                or codes.max() > 127
            ):
                raise ValueError(f"Invalid query embedding for {product}")
            vectors.append(codes.astype(np.int8).tobytes())
            indexed.add(product)
            if len(vectors) == count:
                with output.open("xb") as stream:
                    stream.writelines(vectors)
                return
    raise ValueError("Too few held-out queries in shard")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cache", type=Path)
    parser.add_argument("target", type=Path)
    parser.add_argument(
        "--int8", action="store_true", help="Store symmetric signed int8 embeddings"
    )
    parser.add_argument(
        "--queries-from",
        type=Path,
        metavar="SHARD",
        help="Export held-out int8 queries. Positional arguments are the existing cohorts directory and the output file.",
    )
    parser.add_argument(
        "--query-count", type=int, default=16, help="Number of held-out queries to export"
    )
    args = parser.parse_args()
    if args.queries_from:
        prepare_queries(args.cache, args.queries_from, args.target, args.query_count)
    else:
        prepare(args.cache, args.target, args.int8)
