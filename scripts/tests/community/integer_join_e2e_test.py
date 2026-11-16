"""Compare native $search/$lookup and standalone integer joins against a live MongoDB oracle.

Requires Linux Docker, an existing mongod binary, a source-built mongot package, and the
Python client dependencies. MongoDB data lives in an ephemeral container volume. Search
indexes and credentials are removed on success or failure; logs and results remain in --output.
"""

import argparse
import copy
import hashlib
import os
import secrets
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
import yaml
from bson import Int64, ObjectId, json_util
from coexistence_test import Process, port
from docker_compatibility_test import docker, image_info, wait_for
from mongot_standalone import Client, api, decode_bson, encode_bson
from pymongo import MongoClient
from pymongo.errors import PyMongoError

INTEGER = {
    "type": "number",
    "representation": "int64",
    "indexIntegers": True,
    "indexDoubles": False,
}
MIXED_TYPES = [{"type": "token"}, {"type": "objectId"}, INTEGER]
PARENT_MAPPING = {
    "numPartitions": 2,
    "storedSource": True,
    "mappings": {
        "dynamic": False,
        "fields": {"_id": INTEGER, "office": {"type": "token"}},
    },
}
CHILD_MAPPING = {
    "numPartitions": 2,
    "storedSource": True,
    "mappings": {
        "dynamic": False,
        "fields": {
            "_id": MIXED_TYPES,
            "parent": INTEGER,
            "title": {"type": "string"},
        },
    },
}
JOIN = {
    "join": {
        "from": "parents",
        "localField": "parent",
        "foreignField": "_id",
        "operator": {"equals": {"path": "office", "value": "Berlin"}},
    }
}
QUERY = {
    "compound": {
        "must": [{"text": {"path": "title", "query": "coffee"}}],
        "filter": [JOIN],
    },
    "returnStoredSource": True,
    "count": {"type": "total"},
    "sort": {"_id": 1},
}


def lookup_pipeline(
    search=True, exclude=False, integer_only=True, key_types=None, search_index="children"
):
    first = (
        {"$search": {"index": search_index, "text": {"path": "title", "query": "coffee"}}}
        if search
        else {"$match": {"title": "coffee"}}
    )
    pipeline = [first]
    if integer_only:
        # MongoDB numeric equality also matches doubles; the integer join contract excludes them.
        pipeline.append(
            {
                "$set": {
                    "joinKeys": {
                        "$filter": {
                            "input": {"$cond": [{"$isArray": "$parent"}, "$parent", ["$parent"]]},
                            "as": "key",
                            "cond": {"$in": [{"$type": "$$key"}, key_types or ["int", "long"]]},
                        }
                    }
                }
            }
        )
    pipeline.append(
        {
            "$lookup": {
                "from": "parents",
                "localField": "joinKeys" if integer_only else "parent",
                "foreignField": "_id",
                "as": "parents",
            }
        }
    )
    condition = {"$elemMatch": {"office": "Berlin"}}
    pipeline.extend(
        [
            {"$match": {"parents": {"$not": condition} if exclude else condition}},
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 1, "parentIds": "$parents._id"}},
        ]
    )
    return pipeline


def read_local(client, index_id, query=QUERY):
    response = client.search(
        api.QueryRequest(index_id=index_id, query_bson=encode_bson(query), batch_size=1)
    )
    total = decode_bson(response.metadata_bson)["count"]["total"]
    rows, pages = [], 0
    try:
        while True:
            pages += 1
            rows.extend(decode_bson(response.batch_bson)["results"])
            if not response.cursor_id:
                break
            response = client.get_more(
                api.GetMoreRequest(cursor_id=response.cursor_id, batch_size=1)
            )
    finally:
        if response.cursor_id:
            client.kill_cursor(api.KillCursorRequest(cursor_id=response.cursor_id))
    ids = [row["storedSource"]["_id"] for row in rows]
    assert total == len(ids), (total, ids)
    assert len(ids) == len(set(ids)), ids
    return ids, pages


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix=".integer-join-e2e-", dir=Path.cwd())).resolve()
    search_data = scratch / "indexes"
    search_data.mkdir()
    temporary = scratch / "tmp"
    temporary.mkdir()
    mongo_port, grpc_port = port(), port()
    name = "mongot-integer-e2e-" + secrets.token_hex(5)
    volume = name + "-db"
    results = {"checks": [], "passed": False, "cleanup_errors": []}
    mongo = client = mongot = None
    container_created = volume_created = False

    def passed(name, **details):
        results["checks"].append({"name": name, **details})
        print(f"PASS: {name}", flush=True)

    def local_ready(index_id):
        value = client.get_index(api.GetIndexRequest(index_id=index_id))
        return value.generation_id and value.lifecycle_state == "READY"

    try:
        results["runtime_image"] = image_info(args.runtime_image)
        results["source_head"] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        diff = subprocess.check_output(["git", "diff", "--binary"])
        results["source_diff_sha256"] = hashlib.sha256(diff).hexdigest()
        results["fixture_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        (args.output / "source.diff").write_bytes(diff)
        results["candidate_jar_sha256"] = hashlib.sha256(
            (args.mongot.parent / "bin/mongot_community_deploy.jar").read_bytes()
        ).hexdigest()
        mongo_config = {
            "net": {"bindIp": "127.0.0.1", "port": mongo_port},
            "replication": {"replSetName": "integerjoins"},
            "storage": {
                "dbPath": "/data/db",
                "wiredTiger": {"engineConfig": {"cacheSizeGB": 0.25}},
            },
            "setParameter": {
                "mongotHost": f"127.0.0.1:{grpc_port}",
                "searchIndexManagementHostAndPort": f"127.0.0.1:{grpc_port}",
                "skipAuthenticationToSearchIndexManagementServer": False,
                "useGrpcForSearch": True,
            },
        }
        (scratch / "mongod.yaml").write_text(yaml.safe_dump(mongo_config))
        shutil.copyfile(scratch / "mongod.yaml", args.output / "mongod.yaml")
        docker("volume", "create", volume)
        volume_created = True
        command = ["/mongo/mongod"]
        mounts = ["-v", f"{args.mongod.resolve().parent}:/mongo:ro"]
        if args.mongod_libs:
            mounts += ["-v", f"{args.mongod_libs.resolve()}:/mongo-libs:ro"]
            command = [
                "/mongo-libs/ld-linux-x86-64.so.2",
                "--library-path",
                "/mongo-libs",
                *command,
            ]
        container_created = True
        docker(
            "run",
            "-d",
            "--pull=never",
            "--name",
            name,
            "--network",
            "host",
            "--memory",
            "1g",
            "--cpus",
            "2",
            "-v",
            f"{volume}:/data/db",
            "-v",
            f"{scratch / 'mongod.yaml'}:/mongod.yaml:ro",
            *mounts,
            args.runtime_image,
            *command,
            "--config",
            "/mongod.yaml",
        )
        mongo = MongoClient(
            "127.0.0.1",
            mongo_port,
            directConnection=True,
            serverSelectionTimeoutMS=2000,
            socketTimeoutMS=30000,
        )
        wait_for("MongoDB startup", lambda: mongo.admin.command("ping"))
        mongo.admin.command(
            "replSetInitiate",
            {"_id": "integerjoins", "members": [{"_id": 0, "host": f"127.0.0.1:{mongo_port}"}]},
        )
        wait_for(
            "replica-set primary", lambda: mongo.admin.command("hello").get("isWritablePrimary")
        )
        results["mongodb_version"] = mongo.admin.command("buildInfo")["version"]
        password = scratch / "password"
        password.write_text(secrets.token_hex(24))
        password.chmod(0o400)
        mongo.admin.command(
            "createUser", "searchTest", pwd=password.read_text(), roles=["root", "__system"]
        )
        config = {
            "syncSource": {
                "replicaSet": {
                    "hostAndPort": f"127.0.0.1:{mongo_port}",
                    "scramAuth": {
                        "username": "searchTest",
                        "authSource": "admin",
                        "passwordFile": str(password),
                        "tls": {"enabled": False},
                    },
                }
            },
            "storage": {"dataPath": str(search_data)},
            "server": {"grpc": {"address": f"127.0.0.1:{grpc_port}"}},
            "healthCheck": {"address": f"127.0.0.1:{port()}"},
            "metrics": {"enabled": False, "address": f"127.0.0.1:{port()}"},
        }
        (scratch / "mongot.yaml").write_text(yaml.safe_dump(config))
        shutil.copyfile(scratch / "mongot.yaml", args.output / "mongot.yaml")
        env = dict(
            os.environ,
            JAVA_TOOL_OPTIONS=(
                f"-Xms128m -Xmx768m -XX:ActiveProcessorCount=2 -XX:MaxDirectMemorySize=256m "
                f"-Djava.io.tmpdir={temporary}"
            ),
        )
        mongot = Process(
            [str(args.mongot.resolve()), "--config", str(scratch / "mongot.yaml")],
            args.output / "mongot.log",
            env,
        ).start()
        client = Client(f"127.0.0.1:{grpc_port}", timeout=30)
        wait_for(
            "dual ingestion startup",
            lambda: client.get_status(api.GetStatusRequest()).management_ready,
        )
        db = mongo.integer_join_e2e
        oid = ObjectId("507f1f77bcf86cd799439011")
        parents = [
            {"_id": Int64(-(2**63)), "office": "Berlin"},
            {"_id": Int64(2**63 - 1), "office": "Berlin"},
            {"_id": Int64(2**53 + 1), "office": "Berlin"},
            {"_id": Int64(2**53), "office": "Paris"},
            {"_id": 42, "office": "Berlin"},
            {"_id": Int64(43), "office": "Berlin"},
            {"_id": -7, "office": "Berlin"},
            {"_id": 0, "office": "Berlin"},
            {"_id": "42", "office": "Berlin"},
            {"_id": oid, "office": "Berlin"},
        ]
        parents.append({"_id": str(oid), "office": "Paris"})
        children = [
            {"_id": "min", "parent": Int64(-(2**63))},
            {"_id": "max", "parent": Int64(2**63 - 1)},
            {"_id": "large", "parent": Int64(2**53 + 1)},
            {"_id": "adjacent", "parent": Int64(2**53)},
            {"_id": "wide", "parent": Int64(42)},
            {"_id": "narrow", "parent": 43},
            {"_id": "negative", "parent": -7},
            {"_id": "zero", "parent": Int64(0)},
            {"_id": "array", "parent": [Int64(42), 42, Int64(2**53)]},
            {"_id": "string", "parent": "42"},
            {"_id": "objectid", "parent": oid},
            {"_id": "double", "parent": 42.0},
            {"_id": "fraction", "parent": -7.9},
            {"_id": "orphan", "parent": 999},
            {"_id": "null", "parent": None},
            {"_id": "empty", "parent": []},
            {"_id": "missing"},
        ]
        children.extend(
            [
                {"_id": "hex-string", "parent": str(oid)},
                {"_id": "numeric-string", "parent": "-7"},
                {"_id": "mixed-types", "parent": [999, "42", oid]},
                {"_id": Int64(700), "parent": "42"},
                {"_id": "700", "parent": "42"},
                {"_id": ObjectId("507f1f77bcf86cd799439012"), "parent": "42"},
            ]
        )
        children = [dict(doc, title="coffee") for doc in children]
        children.append({"_id": "nontext", "parent": 42, "title": "tea"})
        db.parents.insert_many(parents)
        db.children.insert_many(children)
        for collection, mapping in (("parents", PARENT_MAPPING), ("children", CHILD_MAPPING)):
            wait_for(
                "native index " + collection,
                lambda collection=collection, mapping=mapping: db.command(
                    {
                        "createSearchIndexes": collection,
                        "indexes": [{"name": collection, "definition": mapping}],
                    }
                ),
            )
        local = {}
        for collection, mapping, documents in (
            ("parents", PARENT_MAPPING, parents),
            ("children", CHILD_MAPPING, children),
        ):
            index = client.create_index(
                api.CreateIndexRequest(
                    name=collection,
                    type=api.INDEX_TYPE_SEARCH,
                    definition_bson=encode_bson(mapping),
                )
            )
            local[collection] = index.index_id
            wait_for(
                "standalone index " + collection,
                lambda index_id=index.index_id: local_ready(index_id),
            )
            client.upsert_many(index.index_id, documents, concern="committed")

        def pipeline_rows(search=True, exclude=False, integer_only=True):
            return list(
                db.children.aggregate(
                    lookup_pipeline(search, exclude, integer_only), batchSize=1, maxTimeMS=15000
                )
            )

        def compare(label):
            oracle = [doc["_id"] for doc in pipeline_rows(search=False)]
            rows = wait_for(
                label + " native pipeline",
                pipeline_rows,
                lambda rows: [r["_id"] for r in rows] == oracle,
            )
            ids, pages = wait_for(
                label + " standalone join",
                lambda: read_local(client, local["children"]),
                lambda value: value[0] == oracle,
            )
            passed(
                label,
                ids=ids,
                standalone_pages=pages,
                mongo_parent_ids=json_util.loads(json_util.dumps(rows)),
            )
            return ids

        initial = compare(
            "initial sync: $search then $lookup agrees with standalone join and MongoDB oracle"
        )
        assert initial == sorted(
            ["min", "max", "large", "wide", "narrow", "negative", "zero", "array"]
        ), initial
        ordinary = [doc["_id"] for doc in pipeline_rows(integer_only=False)]
        assert set(ordinary) == {
            *initial,
            "string",
            "objectid",
            "double",
            "mixed-types",
            700,
            "700",
            ObjectId("507f1f77bcf86cd799439012"),
        }, ordinary
        passed(
            "ordinary $lookup preserves MongoDB string/ObjectId/double equality; integer joins exclude those types"
        )
        exclusion = copy.deepcopy(QUERY)
        exclusion["compound"]["mustNot"] = exclusion["compound"].pop("filter")
        expected = [doc["_id"] for doc in pipeline_rows(search=False, exclude=True)]
        assert [doc["_id"] for doc in pipeline_rows(exclude=True)] == expected
        assert read_local(client, local["children"], exclusion)[0] == expected
        passed("exclusion keeps missing, null, empty, orphan and non-integer keys", ids=expected)

        # A numeric predicate must select adjacent large IDs exactly through the native endpoint.
        parent_pipeline = [
            {"$search": {"index": "parents", "equals": {"path": "_id", "value": Int64(2**53 + 1)}}}
        ]
        wait_for(
            "native exact large ID",
            lambda: list(db.parents.aggregate(parent_pipeline)),
            lambda rows: [r["_id"] for r in rows] == [2**53 + 1],
        )
        explanation = db.command(
            {
                "explain": {"aggregate": "children", "pipeline": lookup_pipeline(), "cursor": {}},
                "verbosity": "executionStats",
            }
        )
        assert "indexPartitionExplain" in str(explanation), explanation
        native_explain = explanation["stages"][0]["$_internalSearchMongotRemote"]["explain"]
        for partition in native_explain["indexPartitionExplain"]:
            assert "title:coffee" in partition["luceneQuery"]["analyzed"]
            assert "title:coffee" in partition["luceneQuery"]["rewritten"]
        (args.output / "pipeline-explain.json").write_text(json_util.dumps(explanation, indent=2))
        passed("native exact int64 equality and partitioned $search/$lookup executionStats explain")

        traced = client.search(
            api.QueryRequest(
                index_id=local["children"],
                query_bson=encode_bson(QUERY),
                batch_size=1,
                explain_verbosity=api.EXPLAIN_VERBOSITY_EXECUTION_STATS,
            )
        )
        first_trace = decode_bson(traced.explain_bson)
        final_trace = first_trace
        previous_counters = {}
        try:
            preparation = first_trace["joins"]["preparation"]
            assert preparation["localPartitions"] == 2, preparation
            assert preparation["keyCount"] > 0, preparation
            assert preparation["keyBytes"] == preparation["keyCount"] * 8, preparation
            assert preparation["reservedBytes"] <= preparation["limits"]["memoryBytes"]
            assert preparation["timing"]["millisElapsed"] >= 0
            source = preparation["sources"][0]
            assert source["foreignIndexId"] == local["parents"], source
            assert source["keyType"] == "INT64" and source["foreignPartitions"] == 2, source
            while True:
                final_trace = decode_bson(traced.explain_bson)
                assert final_trace["joins"]["preparation"] == preparation
                for partition_id, partition in enumerate(final_trace["indexPartitionExplain"]):
                    assert "preparation" not in partition["joins"]
                    for execution in partition["joins"]["execution"]:
                        assert execution["id"] == source["id"]
                        assert execution["strategy"] == "numeric", execution
                        for segment in execution["segments"]:
                            key = (partition_id, execution["id"], segment["ordinal"])
                            counters = {**segment["counters"], **segment["strategyCounts"]}
                            assert all(
                                value >= previous_counters.get(key, {}).get(name, 0)
                                for name, value in counters.items()
                            ), segment
                            previous_counters[key] = counters
                            assert counters["fallbacks"] == 0, segment
                if not traced.cursor_id:
                    break
                traced = client.get_more(
                    api.GetMoreRequest(cursor_id=traced.cursor_id, batch_size=1)
                )
            assert (
                sum(row["points"] + row["docValuesProbe"] for row in previous_counters.values()) > 0
            )
        finally:
            if traced.cursor_id:
                client.kill_cursor(api.KillCursorRequest(cursor_id=traced.cursor_id))
        (args.output / "join-explain-first.json").write_text(json_util.dumps(first_trace, indent=2))
        (args.output / "join-explain-final.json").write_text(json_util.dumps(final_trace, indent=2))
        passed(
            "inline join explain keeps preparation stable and accumulates partition execution across pages"
        )

        mixed = {}
        for collection, mapping, documents in (
            ("parents", PARENT_MAPPING, parents),
            ("children", CHILD_MAPPING, children),
        ):
            definition = copy.deepcopy(mapping)
            definition["mappings"]["fields"]["_id"] = MIXED_TYPES
            if collection == "children":
                definition["mappings"]["fields"]["parent"] = MIXED_TYPES
            db.command(
                {
                    "createSearchIndexes": collection,
                    "indexes": [{"name": "mixed-" + collection, "definition": definition}],
                }
            )
            index = client.create_index(
                api.CreateIndexRequest(
                    name="mixed-" + collection,
                    type=api.INDEX_TYPE_SEARCH,
                    definition_bson=encode_bson(definition),
                )
            )
            mixed[collection] = index.index_id
            wait_for(
                "mixed index " + collection, lambda index_id=index.index_id: local_ready(index_id)
            )
            client.upsert_many(index.index_id, documents, concern="committed")
        mixed_query = copy.deepcopy(QUERY)
        mixed_query["compound"]["filter"][0]["join"]["from"] = "mixed-parents"
        supported_types = ["int", "long", "string", "objectId"]

        def mixed_rows(search=True, exclude=False):
            return list(
                db.children.aggregate(
                    lookup_pipeline(
                        search=search,
                        exclude=exclude,
                        key_types=supported_types,
                        search_index="mixed-children",
                    ),
                    batchSize=1,
                    maxTimeMS=15000,
                )
            )

        def typed_ids(ids):
            return [encode_bson({"_id": value}) for value in ids]

        def compare_mixed(label):
            expected = [row["_id"] for row in mixed_rows(search=False)]
            wait_for(
                label + " native mixed lookup",
                mixed_rows,
                lambda rows: typed_ids([r["_id"] for r in rows]) == typed_ids(expected),
            )
            ids, pages = wait_for(
                label + " standalone mixed join",
                lambda: read_local(client, mixed["children"], mixed_query),
                lambda result: typed_ids(result[0]) == typed_ids(expected),
            )
            excluded_query = copy.deepcopy(mixed_query)
            excluded_query["compound"]["mustNot"] = excluded_query["compound"].pop("filter")
            excluded = [row["_id"] for row in mixed_rows(search=False, exclude=True)]
            wait_for(
                label + " mixed exclusion",
                lambda: read_local(client, mixed["children"], excluded_query),
                lambda result: typed_ids(result[0]) == typed_ids(excluded),
            )
            passed(label, ids=ids, standalone_pages=pages, excluded=excluded)
            return ids

        mixed_initial = compare_mixed(
            "mixed ObjectId/string/integer IDs: pipeline, joins, exclusions and paging"
        )
        assert set(mixed_initial) == set(ordinary) - {"double"}, mixed_initial
        mixed_first = client.search(
            api.QueryRequest(
                index_id=mixed["children"], query_bson=encode_bson(mixed_query), batch_size=1
            )
        )
        assert mixed_first.cursor_id

        # Freeze a local cursor, then change foreign membership and local text on both ingestion paths.
        first = client.search(
            api.QueryRequest(
                index_id=local["children"], query_bson=encode_bson(QUERY), batch_size=1
            )
        )
        assert first.cursor_id
        with ThreadPoolExecutor(max_workers=2) as pool:

            def mongo_writes():
                db.parents.update_one({"_id": 42}, {"$set": {"office": "Paris"}})
                db.parents.update_one({"_id": Int64(2**53)}, {"$set": {"office": "Berlin"}})
                db.parents.delete_one({"_id": Int64(2**63 - 1)})
                db.parents.delete_one({"_id": oid})
                db.parents.update_one({"_id": str(oid)}, {"$set": {"office": "Berlin"}})
                db.children.update_one({"_id": "zero"}, {"$set": {"title": "tea"}})
                db.children.delete_one({"_id": "negative"})
                db.children.insert_one({"_id": "new", "parent": Int64(43), "title": "coffee"})

            def standalone_writes():
                for target in (local, mixed):
                    client.upsert_many(
                        target["parents"],
                        [{"_id": 42, "office": "Paris"}, {"_id": Int64(2**53), "office": "Berlin"}],
                        concern="visible",
                    )
                    client.delete(target["parents"], Int64(2**63 - 1), concern="visible")
                    client.upsert_many(
                        target["children"],
                        [
                            {"_id": "zero", "parent": Int64(0), "title": "tea"},
                            {"_id": "new", "parent": Int64(43), "title": "coffee"},
                        ],
                        concern="visible",
                    )
                    client.delete(target["children"], "negative", concern="visible")

                    client.delete(target["parents"], oid, concern="visible")
                    client.upsert(
                        target["parents"], {"_id": str(oid), "office": "Berlin"}, concern="visible"
                    )

            futures = [pool.submit(mongo_writes), pool.submit(standalone_writes)]
            for future in futures:
                future.result(timeout=60)
        snapshot_ids = [
            doc["storedSource"]["_id"] for doc in decode_bson(first.batch_bson)["results"]
        ]
        response = first
        while response.cursor_id:
            response = client.get_more(
                api.GetMoreRequest(cursor_id=response.cursor_id, batch_size=1)
            )
            snapshot_ids.extend(
                doc["storedSource"]["_id"] for doc in decode_bson(response.batch_bson)["results"]
            )
        assert snapshot_ids == initial, snapshot_ids
        mixed_snapshot = [
            r["storedSource"]["_id"] for r in decode_bson(mixed_first.batch_bson)["results"]
        ]
        response = mixed_first
        while response.cursor_id:
            response = client.get_more(
                api.GetMoreRequest(cursor_id=response.cursor_id, batch_size=1)
            )
            mixed_snapshot.extend(
                r["storedSource"]["_id"] for r in decode_bson(response.batch_bson)["results"]
            )
        assert typed_ids(mixed_snapshot) == typed_ids(mixed_initial)
        passed("open standalone cursor preserves initial local snapshot and parent membership")
        changed = compare(
            "concurrent replication and gRPC insert/update/delete converge to the same parent matches"
        )
        assert changed == sorted(["min", "large", "narrow", "array", "adjacent", "new"]), changed
        mixed_changed = compare_mixed(
            "mixed typed mutations preserve identity and do not cross-match IDs"
        )
        assert "wide" not in mixed_changed and "string" in mixed_changed
        assert "objectid" not in mixed_changed and "hex-string" in mixed_changed
        # Require replicated parent changes too: $lookup itself reads parents directly from mongod.
        parent_query = [
            {"$search": {"index": "parents", "equals": {"path": "office", "value": "Berlin"}}},
            {"$project": {"_id": 1}},
        ]
        expected_parent_ids = {
            encode_bson({"_id": doc["_id"]}) for doc in db.parents.find({"office": "Berlin"})
        }
        wait_for(
            "parent replication after mutations",
            lambda: list(db.parents.aggregate(parent_query)),
            lambda rows: {encode_bson({"_id": r["_id"]}) for r in rows} == expected_parent_ids,
        )
        passed("parent search index independently reflects updates and deletes")

        mongot.stop()
        mongot.start()
        wait_for(
            "restarted standalone services",
            lambda: client.get_status(api.GetStatusRequest()).management_ready,
        )
        for index_id in [*local.values(), *mixed.values()]:
            wait_for("restarted index", lambda index_id=index_id: local_ready(index_id))
        assert compare("restart retains replicated and standalone integer joins") == changed
        assert typed_ids(compare_mixed("mixed-ID joins recover after restart")) == typed_ids(
            mixed_changed
        )
        docker("stop", "--time", "30", name)
        client.upsert(
            local["children"],
            {"_id": "offline", "parent": Int64(43), "title": "coffee"},
            concern="visible",
        )
        assert read_local(client, local["children"])[0] == sorted([*changed, "offline"])
        passed("standalone integer joins and writes remain available during MongoDB outage")
        docker("start", name)
        wait_for("MongoDB recovery", lambda: mongo.admin.command("hello").get("isWritablePrimary"))
        db.children.insert_one({"_id": "offline", "parent": Int64(43), "title": "coffee"})
        assert compare("replication resumes after source outage") == sorted([*changed, "offline"])
        client.upsert(
            mixed["children"],
            {"_id": "offline", "parent": Int64(43), "title": "coffee"},
            concern="visible",
        )
        compare_mixed("mixed-ID joins converge again after MongoDB outage")
        results["passed"] = True
    except Exception as error:
        results["failure"] = repr(error)
        raise
    finally:

        def cleanup(action):
            try:
                action()
            except (
                OSError,
                RuntimeError,
                subprocess.SubprocessError,
                PyMongoError,
                grpc.RpcError,
                AssertionError,
            ) as error:
                results["cleanup_errors"].append(repr(error))

        if client:
            cleanup(client.close)
        if mongot:
            cleanup(lambda: mongot.stop(verify=results["passed"]))
        if mongo:
            cleanup(mongo.close)
        if container_created:
            cleanup(lambda: (args.output / "mongod.log").write_text(docker("logs", name)))
            cleanup(lambda: docker("rm", "--force", "--volumes", name))
        if volume_created:
            cleanup(lambda: docker("volume", "rm", volume))
        cleanup(lambda: shutil.rmtree(scratch))
        if results["cleanup_errors"]:
            results["passed"] = False
        (args.output / "results.json").write_text(json_util.dumps(results, indent=2) + "\n")
        print(f"Results: {args.output / 'results.json'}", flush=True)
        if results["cleanup_errors"]:
            raise RuntimeError(f"Fixture cleanup failed: {results['cleanup_errors']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongod", type=Path, required=True)
    parser.add_argument(
        "--mongod-libs", type=Path, help="Optional cached Linux runtime libraries and loader"
    )
    parser.add_argument("--mongot", type=Path, required=True)
    parser.add_argument(
        "--runtime-image", default="ubuntu:24.04", help="Preloaded Linux image; never pulled"
    )
    parser.add_argument("--output", type=Path, required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
