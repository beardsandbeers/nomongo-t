#!/usr/bin/env python3
"""Compare live MongoDB $searchMeta stages with the standalone RPC using one engine build.

Requires Docker and the installed Python client. Uses only existing images, a private Docker
network/volumes, temporary standalone storage and ephemeral localhost ports. Never touches an
existing container. MongoDB's version and every image ID are recorded and checked.
"""

import argparse
import hashlib
import json
import os
import secrets
import shlex
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import grpc
from bson import Int64, json_util
from e2e import Server
from mongot_standalone import Client, api, decode_bson, encode_bson
from pymongo import MongoClient
from pymongo.errors import OperationFailure

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/tests/community"))
from docker_compatibility_test import (
    MONGOD_CONFIG,
    MONGOT_CONFIG,
    docker,
    image_info,
    mapped_port,
    wait_for,
)


def canonical(value):
    # Retain numeric BSON types and array ordering; ignore only document field ordering.
    return json_util.dumps(value, json_options=json_util.CANONICAL_JSON_OPTIONS, sort_keys=True)


def engine_digest(path):
    # The two launcher manifests differ; compare their engine bytecode, not archive timestamps.
    digest = hashlib.sha256()
    with ZipFile(path) as archive:
        for name in sorted(archive.namelist()):
            if name.startswith("com/xgen/mongot/") and name.endswith(".class"):
                digest.update(name.encode())
                digest.update(archive.read(name))
    return digest.hexdigest()


def cases():
    text = {"text": {"path": "title", "query": "coffee"}}
    facets = {
        "brands": {"type": "string", "path": "brand", "numBuckets": 2},
        "prices": {
            "type": "number",
            "path": "price",
            "boundaries": [0, 3, 10, 100],
            "default": "other",
        },
        "dates": {
            "type": "date",
            "path": "created",
            "boundaries": [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1)],
            "default": "other",
        },
    }
    facet = {"facet": {"operator": text, "facets": facets}}
    metrics = {
        "facet": {
            "operator": text,
            "facets": {
                "prices": {
                    "type": "number",
                    "path": "price",
                    "boundaries": [0, 10, 100],
                    "aggregations": {
                        "sum": {"type": "sum", "path": "price"},
                        "first": {"type": "first", "path": "price"},
                        "last": {"type": "last", "path": "price"},
                    },
                },
            },
        },
        "sort": {"price": -1},
    }
    vector = {"path": "embedding", "queryVector": [1.0, 0.0, 0.0], "limit": 2}
    return [
        ("default_count", text, True),
        ("explicit_index", dict(text, index="default"), True),
        ("exact_count", dict(text, count={"type": "total"}), True),
        ("lower_bound_count", dict(text, count={"type": "lowerBound", "threshold": 1}), True),
        ("zero_matches", {"text": {"path": "title", "query": "absent"}}, True),
        ("facets_with_default_count", facet, True),
        ("facets_exact_count", dict(facet, count={"type": "total"}), True),
        (
            "facets_lower_bound_threshold",
            dict(facet, count={"type": "lowerBound", "threshold": 1}),
            True,
        ),
        ("facets_without_operator", {"facet": {"facets": facets}}, True),
        (
            "facets_zero_matches",
            {
                "facet": {
                    "operator": {"text": {"path": "title", "query": "absent"}},
                    "facets": facets,
                }
            },
            True,
        ),
        ("legacy_sort_ignored", dict(facet, sort={"price": -1}), True),
        ("sorted_metrics", metrics, True),
        (
            "highlight_and_score_details",
            dict(text, highlight={"path": "title"}, scoreDetails=True),
            True,
        ),
        ("hit_sort_ignored", dict(text, sort={"price": 1}), True),
        ("beta_sort_ignored", dict(text, sortBetaV1={"price": 1}), True),
        ("tracking", dict(text, tracking={"searchTerms": "coffee"}), True),
        ("search_node_preference", dict(text, searchNodePreference={"key": "parity"}), True),
        ("invalid_tracking", dict(text, tracking={}), False),
        ("invalid_search_node_preference", dict(text, searchNodePreference={"key": ""}), False),
        ("stored_source_false", dict(text, returnStoredSource=False), True),
        ("stored_source_unconfigured", dict(text, returnStoredSource=True), False),
        ("concurrent_facets", dict(facet, concurrent=True), True),
        (
            "drill_sideways",
            {
                "facet": {
                    "operator": {
                        "compound": {
                            "must": [text],
                            "filter": [
                                {
                                    "compound": {
                                        "filter": [{"equals": {"path": "brand", "value": "Acme"}}],
                                        "doesNotAffect": ["brands"],
                                    }
                                }
                            ],
                        }
                    },
                    "facets": facets,
                }
            },
            True,
        ),
        ("missing_index", dict(text, index="absent"), True),
        ("missing_index_facets", dict(facet, index="absent"), True),
        (
            "vector_exact",
            {"vectorSearch": dict(vector, exact=True), "count": {"type": "total"}},
            True,
        ),
        ("vector_explicit_candidates", {"vectorSearch": dict(vector, numCandidates=10)}, True),
        ("vector_auto_candidates_disabled", {"vectorSearch": vector}, False),
        (
            "return_scope",
            {
                "exists": {"path": "variants.price"},
                "returnScope": {"path": "variants"},
                "count": {"type": "total"},
            },
            True,
        ),
        ("empty_query", {}, False),
        ("unknown_field", dict(text, unknown=True), False),
        ("operator_and_collector", dict(text, **facet), False),
        ("empty_facets", {"facet": {"facets": {}}}, False),
        (
            "bucket_limit_disabled",
            {
                "facet": {
                    "facets": {"brands": {"type": "string", "path": "brand", "numBuckets": 2000}}
                }
            },
            False,
        ),
        ("malformed_pagination", dict(text, searchAfter="invalid"), False),
        ("unknown_field_missing_index", dict(text, index="absent", unknown=True), False),
    ]


def compare(collection, client, name, body, accepted):
    result = {"case": name, "query": json_util.loads(canonical(body))}
    try:
        coupled = list(collection.aggregate([{"$searchMeta": body}], maxTimeMS=15000))
        result["coupled"] = {"accepted": True, "metadata": coupled}
    except OperationFailure as error:
        result["coupled"] = {
            "accepted": False,
            "code": error.code,
            "code_name": (error.details or {}).get("codeName"),
            "message": str(error),
            "query_error": (error.details or {})
            .get("errmsg", "")
            .rsplit(" :: caused by :: ", 1)[-1],
        }
    try:
        response = client.search_meta(api.SearchMetaRequest(query_bson=encode_bson(body)))
        metadata = (
            [decode_bson(response.metadata_bson)] if response.HasField("metadata_bson") else []
        )
        result["standalone"] = {"accepted": True, "metadata": metadata}
    except grpc.RpcError as error:
        result["standalone"] = {
            "accepted": False,
            "status": error.code().name,
            "message": error.details(),
        }
    result["passed"] = result["coupled"]["accepted"] == result["standalone"][
        "accepted"
    ] == accepted and (
        canonical(result["coupled"]["metadata"]) == canonical(result["standalone"]["metadata"])
        if accepted
        else result["standalone"]["status"] == "INVALID_ARGUMENT"
        and result["standalone"]["message"] == result["coupled"]["query_error"]
        and result["coupled"]["code_name"]
        not in {"InternalError", "HostUnreachable", "CommandNotFound"}
    )
    return result


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix=".search-meta-parity-", dir=ROOT))
    scratch.chmod(0o755)
    suffix = secrets.token_hex(6)
    network, source, search = [
        f"search-meta-parity-{suffix}-{part}" for part in ("net", "mongo", "mongot")
    ]
    volumes = [f"search-meta-parity-{suffix}-{part}" for part in ("database", "configdb", "search")]
    created_containers, created_volumes = [], []
    created_network = False
    mongo = None
    report = {
        "passed": False,
        "cases": [],
        "explain": [],
        "engine_defaults": "community defaults on both transports",
    }

    def save():
        (args.output / "results.json").write_text(json_util.dumps(report, indent=2) + "\n")

    try:
        report["images"] = {
            "mongo": image_info(args.mongodb_image),
            "runtime": image_info(args.runtime_image),
        }
        report["engine_sha256"] = engine_digest(args.package / "bin/mongot_community_deploy.jar")
        assert report["engine_sha256"] == engine_digest(
            args.package / "bin/mongot_standalone_deploy.jar"
        ), "Launchers must use the same engine build"
        report["existing_containers"] = docker("ps", "--format", "{{.ID}} {{.Names}} {{.Status}}")
        (scratch / "mongod.conf").write_text(MONGOD_CONFIG)
        (scratch / "mongot.conf").write_text(MONGOT_CONFIG)
        password = secrets.token_hex(24)
        (scratch / "passwordFile").write_text(password)
        (scratch / "passwordFile").chmod(0o400)
        docker("network", "create", network)
        created_network = True
        for volume in volumes:
            docker("volume", "create", volume)
            created_volumes.append(volume)
        created_containers.append(source)
        docker(
            "run",
            "-d",
            "--name",
            source,
            "--network",
            network,
            "--network-alias",
            "mongod.search-community",
            "--memory",
            "1g",
            "--cpus",
            "1",
            "-v",
            f"{volumes[0]}:/data/db",
            "-v",
            f"{volumes[1]}:/data/configdb",
            "-v",
            f"{scratch / 'mongod.conf'}:/etc/mongod.conf:ro",
            "-p",
            "127.0.0.1::27017",
            args.mongodb_image,
            "mongod",
            "--config",
            "/etc/mongod.conf",
            "--bind_ip_all",
            "--wiredTigerCacheSizeGB",
            "0.25",
        )
        mongo = MongoClient(
            "127.0.0.1",
            mapped_port(source, 27017),
            directConnection=True,
            serverSelectionTimeoutMS=1000,
            socketTimeoutMS=20000,
        )
        wait_for("MongoDB startup", lambda: mongo.admin.command("ping"))
        report["mongodb_version"] = mongo.admin.command("buildInfo")["version"]
        assert report["mongodb_version"] == args.mongodb_version, report["mongodb_version"]
        mongo.admin.command(
            "replSetInitiate",
            {"_id": "rs0", "members": [{"_id": 0, "host": "mongod.search-community:27017"}]},
        )
        wait_for("MongoDB primary", lambda: mongo.admin.command("hello").get("isWritablePrimary"))
        mongo.admin.command("createUser", "mongotUser", pwd=password, roles=["searchCoordinator"])
        created_containers.append(search)
        docker(
            "run",
            "-d",
            "--name",
            search,
            "--network",
            network,
            "--network-alias",
            "mongot-community.search-community",
            "--user",
            "0:0",
            "--memory",
            "2g",
            "--cpus",
            "2",
            "-e",
            "JAVA_TOOL_OPTIONS=-Xms128m -Xmx768m -XX:ActiveProcessorCount=2",
            "-v",
            f"{args.package}:/opt/mongot:ro",
            "-v",
            f"{volumes[2]}:/data/mongot",
            "-v",
            f"{scratch / 'mongot.conf'}:/mongot.conf:ro",
            "-v",
            f"{scratch / 'passwordFile'}:/passwordFile:ro",
            "--entrypoint",
            "/opt/mongot/mongot",
            args.runtime_image,
            "--config",
            "/mongot.conf",
        )

        launcher = scratch / "standalone"
        launcher.write_text(
            "#!/bin/sh\nexport JAVA_TOOL_OPTIONS='-Xms128m -Xmx768m -XX:ActiveProcessorCount=2'\nexec "
            + shlex.quote(str(args.package / "mongot-standalone"))
            + ' "$@"\n'
        )
        launcher.chmod(0o755)
        standalone_dir = args.output / "standalone"
        standalone_dir.mkdir()
        with (
            Server(launcher, standalone_dir, True) as server,
            Client(server.address, timeout=30) as client,
        ):
            for partitions in args.partitions:
                collection = mongo[f"search_meta_parity_p{partitions}"].records
                documents = [
                    {
                        "_id": "a",
                        "title": "coffee",
                        "brand": ["Acme", "Acme"],
                        "price": Int64(2),
                        "created": datetime(2024, 1, 2),
                        "embedding": [1.0, 0.0, 0.0],
                        "variants": [{"price": 2}, {"price": 3}],
                    },
                    {
                        "_id": "b",
                        "title": "coffee",
                        "brand": "Acme",
                        "price": Int64(3),
                        "created": datetime(2024, 1, 3),
                        "embedding": [0.9, 0.1, 0.0],
                    },
                    {
                        "_id": "c",
                        "title": "coffee",
                        "brand": "Brew",
                        "price": Int64(4),
                        "created": datetime(2024, 2, 2),
                        "embedding": [0.0, 1.0, 0.0],
                    },
                    {
                        "_id": "d",
                        "title": "tea",
                        "brand": "Other",
                        "price": Int64(20),
                        "created": datetime(2024, 3, 2),
                    },
                    {
                        "_id": "e",
                        "title": "coffee",
                        "brand": ["Brew", "Cedar"],
                        "price": Int64(25),
                        "created": datetime(2024, 5, 2),
                    },
                    {
                        "_id": "f",
                        "title": "coffee",
                        "price": Int64(-1),
                        "created": datetime(2024, 1, 1),
                    },
                ]
                definition = {
                    "numPartitions": partitions,
                    "mappings": {
                        "dynamic": False,
                        "fields": {
                            "title": {"type": "string"},
                            "brand": {"type": "token"},
                            "price": {"type": "number"},
                            "created": {"type": "date"},
                            "embedding": {
                                "type": "vector",
                                "numDimensions": 3,
                                "similarity": "cosine",
                            },
                            "variants": {
                                "type": "embeddedDocuments",
                                "dynamic": True,
                                "storedSource": True,
                            },
                        },
                    },
                }
                collection.insert_many(documents)
                wait_for(
                    "coupled index creation",
                    lambda: collection.database.command(
                        {
                            "createSearchIndexes": "records",
                            "indexes": [{"name": "default", "definition": definition}],
                        }
                    ),
                )
                wait_for(
                    "replicated documents",
                    lambda: list(
                        collection.aggregate(
                            [
                                {
                                    "$searchMeta": {
                                        "exists": {"path": "title"},
                                        "count": {"type": "total"},
                                    }
                                }
                            ],
                            maxTimeMS=10000,
                        )
                    ),
                    lambda rows: bool(rows) and rows[0]["count"]["total"] == len(documents),
                )
                index = client.create_index(
                    api.CreateIndexRequest(
                        name="default",
                        type=api.INDEX_TYPE_SEARCH,
                        definition_bson=encode_bson(definition),
                    )
                )
                wait_for(
                    "standalone index",
                    lambda: (
                        client.get_index(
                            api.GetIndexRequest(index_id=index.index_id)
                        ).lifecycle_state
                        == "READY"
                    ),
                )
                client.upsert_many(index.index_id, documents, concern="visible")
                for name, body, accepted in cases():
                    result = compare(collection, client, name, body, accepted)
                    result["partitions"] = partitions
                    report["cases"].append(result)
                    save()
                    print(
                        f"{'PASS' if result['passed'] else 'FAIL'} p{partitions}/{name}", flush=True
                    )
                body = {"text": {"path": "title", "query": "coffee"}}
                for mongo_verbosity, rpc_verbosity in [
                    ("queryPlanner", api.EXPLAIN_VERBOSITY_QUERY_PLANNER),
                    ("executionStats", api.EXPLAIN_VERBOSITY_EXECUTION_STATS),
                    ("allPlansExecution", api.EXPLAIN_VERBOSITY_ALL_PLANS_EXECUTION),
                ]:
                    explanation = collection.database.command(
                        {
                            "explain": {
                                "aggregate": "records",
                                "pipeline": [{"$searchMeta": body}],
                                "cursor": {},
                            },
                            "verbosity": mongo_verbosity,
                        }
                    )
                    response = client.search_meta(
                        api.SearchMetaRequest(
                            query_bson=encode_bson(body), explain_verbosity=rpc_verbosity
                        )
                    )
                    assert response.explain_bson and "mongot" in str(explanation).lower(), (
                        explanation
                    )
                    assert response.HasField("metadata_bson") == (mongo_verbosity != "queryPlanner")
                    report["explain"].append(
                        {"partitions": partitions, "verbosity": mongo_verbosity, "passed": True}
                    )
                client.drop_index(
                    api.DropIndexRequest(
                        index_id=index.index_id, expected_revision=index.desired_revision
                    )
                )
                empty = collection.database.create_collection("empty")
                empty.database.command(
                    {
                        "createSearchIndexes": "empty",
                        "indexes": [{"name": "empty", "definition": definition}],
                    }
                )
                wait_for(
                    "coupled empty index",
                    lambda: list(empty.aggregate([{"$listSearchIndexes": {"name": "empty"}}])),
                    lambda rows: bool(rows) and rows[0].get("queryable"),
                )
                empty_index = client.create_index(
                    api.CreateIndexRequest(
                        name="empty",
                        type=api.INDEX_TYPE_SEARCH,
                        definition_bson=encode_bson(definition),
                    )
                )
                wait_for(
                    "standalone empty index",
                    lambda: (
                        client.get_index(
                            api.GetIndexRequest(index_id=empty_index.index_id)
                        ).lifecycle_state
                        == "READY"
                    ),
                )
                for name, body in [
                    ("empty_index_count", {"text": {"path": "title", "query": "coffee"}}),
                    (
                        "empty_index_facets",
                        {"facet": {"facets": {"brands": {"type": "string", "path": "brand"}}}},
                    ),
                ]:
                    result = compare(empty, client, name, dict(body, index="empty"), True)
                    result["partitions"] = partitions
                    report["cases"].append(result)
                    save()
                    print(
                        f"{'PASS' if result['passed'] else 'FAIL'} p{partitions}/{name}", flush=True
                    )
                client.drop_index(
                    api.DropIndexRequest(
                        index_id=empty_index.index_id,
                        expected_revision=empty_index.desired_revision,
                    )
                )
        report["passed"] = all(row["passed"] for row in report["cases"])
        assert report["passed"], "Stage/RPC mismatch; inspect results.json"
    except Exception as error:
        report["failure"] = str(error)
        raise
    finally:
        if mongo is not None:
            mongo.close()
        cleanup_errors = []
        for container in reversed(created_containers):
            if docker("container", "inspect", container, check=False):
                (args.output / f"{container}.log").write_text(
                    docker("logs", container, check=False)
                )
                try:
                    docker("rm", "--force", "--volumes", container)
                except (RuntimeError, subprocess.SubprocessError) as error:
                    cleanup_errors.append(str(error))
        for volume in created_volumes:
            try:
                docker("volume", "rm", volume)
            except (RuntimeError, subprocess.SubprocessError) as error:
                cleanup_errors.append(str(error))
        if created_network:
            try:
                docker("network", "rm", network)
            except (RuntimeError, subprocess.SubprocessError) as error:
                cleanup_errors.append(str(error))
        shutil.rmtree(scratch)
        report["cleanup_errors"] = cleanup_errors
        if cleanup_errors:
            report["passed"] = False
        report["containers_after"] = docker("ps", "--format", "{{.ID}} {{.Names}} {{.Status}}")
        save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--package", type=Path, required=True, help="Extracted current mongot-standalone package"
    )
    parser.add_argument("--mongodb-image", default="mongo:8.3.17-noble")
    parser.add_argument("--mongodb-version", default="8.3.17")
    parser.add_argument(
        "--runtime-image",
        required=True,
        help="Existing image providing runtime system libraries; /opt/mongot is replaced by --package",
    )
    parser.add_argument("--partitions", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.package = args.package.resolve()
    args.output = args.output.resolve()
    run(args)


if __name__ == "__main__":
    main()
