#!/usr/bin/env python3
"""Exercise direct reads over genuinely replicated indexes and preserve mutation ownership.

Uses existing Docker images, isolated containers/volumes, and ephemeral loopback ports. Linux
host networking allows the native and direct clients to use the same loopback-only listener.
"""

import argparse
import json
import secrets
import shutil
import socket
import sys
import tempfile
from pathlib import Path

import grpc
from mongot_standalone import Client, api, decode_bson, encode_bson
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/tests/community"))
from docker_compatibility_test import MONGOD_CONFIG, MONGOT_CONFIG, docker, wait_for


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def expect_status(code, action):
    try:
        action()
    except grpc.RpcError as error:
        assert error.code() == code, (error.code(), error.details())
    else:
        raise AssertionError(f"Expected {code}")


def exercise(client, mongo, passed, source, search):
    collection = mongo.direct_reads.records
    documents = [
        {"_id": n, "title": "coffee beans", "brand": "Acme", "vector": [1.0, 0.0, 0.0]}
        for n in range(20)
    ]
    collection.insert_many(documents)
    definition = {
        "storedSource": True,
        "mappings": {
            "dynamic": False,
            "fields": {
                "title": [{"type": "string"}, {"type": "autocomplete"}],
                "brand": {"type": "token"},
            },
        },
    }
    collection.create_search_index({"name": "default", "definition": definition})
    count_body = {"exists": {"path": "title"}, "count": {"type": "total"}}
    wait_for(
        "replication",
        lambda: list(collection.aggregate([{"$searchMeta": count_body}])),
        lambda rows: bool(rows) and rows[0]["count"]["total"] == 20,
    )
    indexes = client.list_indexes(api.ListIndexesRequest()).indexes
    remote = next(
        index
        for index in indexes
        if index.ownership == api.INDEX_OWNERSHIP_REPLICATED
        and index.namespace.database == "direct_reads"
    )
    assert remote.queryable and remote.name == "default" and remote.namespace.collection_uuid
    assert remote.active_version.HasField("definition_version")
    passed("replicated discovery and namespace")

    push = client.create_index(
        api.CreateIndexRequest(
            name="default", type=api.INDEX_TYPE_SEARCH, definition_bson=encode_bson(definition)
        )
    )
    wait_for(
        "push readiness",
        lambda: client.get_index(api.GetIndexRequest(index_id=push.index_id)).queryable,
    )
    client.upsert_many(push.index_id, documents[:2], concern="visible")
    page = client.list_indexes(api.ListIndexesRequest(page_size=1))
    assert len(page.indexes) == 1 and page.next_page_token
    second = client.list_indexes(
        api.ListIndexesRequest(page_size=1, page_token=page.next_page_token)
    )
    assert {page.indexes[0].index_id, second.indexes[0].index_id} == {
        push.index_id,
        remote.index_id,
    }
    assert (
        len(
            client.list_indexes(
                api.ListIndexesRequest(ownership=api.INDEX_OWNERSHIP_STANDALONE)
            ).indexes
        )
        == 1
    )
    passed("both owners, duplicate names, filters, and pagination")

    mappings = client.get_index_mappings(api.GetIndexMappingsRequest(index_id=remote.index_id))
    assert "title" in json.loads(mappings.mappings_json)["fields"]
    stats = client.get_index_stats(api.GetIndexStatsRequest(index_id=remote.index_id, refresh=True))
    assert stats.document_count == 20 and stats.generation_id == mappings.generation_id
    fields = wait_for(
        "field statistics",
        lambda: client.get_index_field_stats(
            api.GetIndexFieldStatsRequest(index_id=remote.index_id, page_size=1)
        ),
        lambda value: value.state == api.STATS_COLLECTION_STATE_READY,
    )
    assert fields.index_stats.document_count == 20 and fields.fields
    passed("replicated mappings and statistics")

    legacy = client.search_meta(api.SearchMetaRequest(query_bson=encode_bson(count_body)))
    explicit = client.search_meta(
        api.SearchMetaRequest(target_index_id=remote.index_id, query_bson=encode_bson(count_body))
    )
    assert decode_bson(legacy.metadata_bson)["count"]["total"] == 2
    assert (
        decode_bson(explicit.metadata_bson)
        == list(collection.aggregate([{"$searchMeta": count_body}]))[0]
    )
    autocomplete = client.search(
        api.QueryRequest(
            index_id=remote.index_id,
            query_bson=encode_bson({"autocomplete": {"path": "title", "query": "cof"}}),
        )
    )
    assert len(decode_bson(autocomplete.batch_bson)["results"]) == 20
    passed("SearchMeta target selection, native parity, and autocomplete")

    query = api.QueryRequest(
        index_id=remote.index_id,
        batch_size=2,
        query_bson=encode_bson(
            {"text": {"path": "title", "query": "coffee"}, "returnStoredSource": True}
        ),
        explain_verbosity=api.EXPLAIN_VERBOSITY_EXECUTION_STATS,
    )
    first = client.search(query)
    assert first.cursor_id and len(decode_bson(first.batch_bson)["results"]) == 2
    assert first.explain_bson
    more = client.get_more(api.GetMoreRequest(cursor_id=first.cursor_id, batch_size=2))
    assert more.generation_id == first.generation_id and more.cursor_id
    client.kill_cursor(api.KillCursorRequest(cursor_id=more.cursor_id))
    expect_status(
        grpc.StatusCode.NOT_FOUND,
        lambda: client.get_more(api.GetMoreRequest(cursor_id=more.cursor_id)),
    )
    passed("direct replicated cursor paging, explain, and kill")

    for root in (push.index_id, remote.index_id):
        for foreign in (push.index_id, remote.index_id):
            joined = client.search(
                api.QueryRequest(
                    index_id=root,
                    query_bson=encode_bson(
                        {
                            "compound": {
                                "filter": [
                                    {
                                        "join": {
                                            "from": "inventory",
                                            "localField": "brand",
                                            "foreignField": "brand",
                                            "operator": {"exists": {"path": "brand"}},
                                        }
                                    }
                                ]
                            }
                        }
                    ),
                    join_indexes=[api.JoinIndexBinding(alias="inventory", index_id=foreign)],
                )
            )
            assert len(decode_bson(joined.batch_bson)["results"]) == (
                2 if root == push.index_id else 20
            )
    passed("all four join ownership combinations")

    for action in (
        lambda: client.update_index(
            api.UpdateIndexRequest(
                index_id=remote.index_id,
                expected_revision=1,
                definition_bson=encode_bson(definition),
            )
        ),
        lambda: client.drop_index(
            api.DropIndexRequest(index_id=remote.index_id, expected_revision=1)
        ),
        lambda: client.upsert_request(
            api.UpsertRequest(index_id=remote.index_id, documents_bson=[encode_bson(documents[0])])
        ),
        lambda: client.delete_request(
            api.DeleteRequest(index_id=remote.index_id, ids_bson=[encode_bson({"_id": 0})])
        ),
        lambda: client.delete_by_query(
            api.DeleteByQueryRequest(index_id=remote.index_id, match_all=True)
        ),
        lambda: client.delete_by_query(
            api.DeleteByQueryRequest(index_id=remote.index_id, match_all=True, dry_run=True)
        ),
    ):
        expect_status(grpc.StatusCode.NOT_FOUND, action)
    passed("all direct mutations reject replicated ownership")

    collection.create_search_index(
        {
            "name": "vectors",
            "type": "vectorSearch",
            "definition": {
                "fields": [
                    {"type": "vector", "path": "vector", "numDimensions": 3, "similarity": "cosine"}
                ]
            },
        }
    )
    vector = wait_for(
        "vector discovery",
        lambda: next(
            (
                index
                for index in client.list_indexes(api.ListIndexesRequest()).indexes
                if index.name == "vectors" and index.queryable
            ),
            None,
        ),
    )
    vector_mapping = client.get_index_mappings(
        api.GetIndexMappingsRequest(index_id=vector.index_id)
    )
    assert json.loads(vector_mapping.vector_fields_json)[0]["path"] == "vector"
    vectors = client.vector_search(
        api.QueryRequest(
            index_id=vector.index_id,
            query_bson=encode_bson(
                {"path": "vector", "queryVector": [1.0, 0.0, 0.0], "exact": True, "limit": 3}
            ),
        )
    )
    assert len(decode_bson(vectors.batch_bson)["results"]) == 3
    passed("replicated vector inspection and queries")

    mongo.direct_reads.command(
        "create",
        "coffee_view",
        viewOn="records",
        pipeline=[{"$match": {"$expr": {"$lt": ["$_id", 5]}}}],
    )
    view = mongo.direct_reads.coffee_view
    view.create_search_index({"name": "view_default", "definition": definition})
    view_index = wait_for(
        "view index",
        lambda: next(
            (
                index
                for index in client.list_indexes(api.ListIndexesRequest()).indexes
                if index.namespace.view_name == "coffee_view" and index.queryable
            ),
            None,
        ),
    )
    view_counts = client.search_meta(
        api.SearchMetaRequest(
            target_index_id=view_index.index_id, query_bson=encode_bson(count_body)
        )
    )
    assert decode_bson(view_counts.metadata_bson)["count"]["total"] == 5
    passed("replicated view selection and pipeline semantics")

    mongo.direct_reads.synonyms.insert_one(
        {"mappingType": "equivalent", "synonyms": ["coffee", "java"]}
    )
    synonym_definition = {
        "mappings": {"dynamic": True},
        "synonyms": [
            {
                "name": "coffee_terms",
                "analyzer": "lucene.standard",
                "source": {"collection": "synonyms"},
            }
        ],
    }
    collection.create_search_index({"name": "with_synonyms", "definition": synonym_definition})
    synonym_index = wait_for(
        "synonym index",
        lambda: next(
            (
                index
                for index in client.list_indexes(api.ListIndexesRequest()).indexes
                if index.name == "with_synonyms" and index.queryable
            ),
            None,
        ),
    )
    synonyms = client.get_index_mappings(
        api.GetIndexMappingsRequest(index_id=synonym_index.index_id)
    )
    assert json.loads(synonyms.definition_json)["synonyms"][0]["name"] == "coffee_terms"
    wait_for(
        "synonym query",
        lambda: client.search(
            api.QueryRequest(
                index_id=synonym_index.index_id,
                query_bson=encode_bson(
                    {"text": {"path": "title", "query": "java", "synonyms": "coffee_terms"}}
                ),
            )
        ),
        lambda response: len(decode_bson(response.batch_bson)["results"]) == 20,
    )
    passed("native synonyms preserved in definitions and query execution")

    docker("stop", "--time", "20", source)
    try:
        assert client.get_index(api.GetIndexRequest(index_id=remote.index_id)).queryable
        assert len(decode_bson(client.search(query).batch_bson)["results"]) == 2
        assert (
            client.get_index_stats(
                api.GetIndexStatsRequest(index_id=remote.index_id, refresh=True)
            ).document_count
            == 20
        )
        client.upsert(
            push.index_id,
            {"_id": 2, "title": "offline coffee", "brand": "Acme"},
            concern="committed",
        )
        passed("replicated reads and standalone writes during source outage")
        docker("restart", "--time", "30", search)
        wait_for(
            "offline service recovery",
            lambda: client.get_index(api.GetIndexRequest(index_id=remote.index_id)).queryable,
        )
        assert len(decode_bson(client.search(query).batch_bson)["results"]) == 2
        assert client.get_index(api.GetIndexRequest(index_id=push.index_id)).queryable
        passed("offline process restart restores both owners")
    finally:
        docker("start", source)
    wait_for(
        "source primary recovery", lambda: mongo.admin.command("hello").get("isWritablePrimary")
    )
    collection.insert_one(
        {"_id": 20, "title": "coffee beans", "brand": "Acme", "vector": [1.0, 0.0, 0.0]}
    )
    wait_for(
        "resumed replication",
        lambda: client.search_meta(
            api.SearchMetaRequest(
                target_index_id=remote.index_id, query_bson=encode_bson(count_body)
            )
        ),
        lambda response: decode_bson(response.metadata_bson)["count"]["total"] == 21,
    )
    passed("replication resumes after source recovery")

    cursor = client.search(query)
    replacement = dict(definition, numPartitions=2)
    collection.update_search_index("default", replacement)
    new = wait_for(
        "replacement generation",
        lambda: client.get_index(api.GetIndexRequest(index_id=remote.index_id)),
        lambda value: value.queryable and value.generation_id != cursor.generation_id,
    )
    expect_status(
        grpc.StatusCode.NOT_FOUND,
        lambda: client.get_more(api.GetMoreRequest(cursor_id=cursor.cursor_id)),
    )
    expect_status(
        grpc.StatusCode.FAILED_PRECONDITION,
        lambda: client.get_index_mappings(
            api.GetIndexMappingsRequest(
                index_id=remote.index_id, generation_id=cursor.generation_id
            )
        ),
    )
    assert (
        client.get_index_stats(api.GetIndexStatsRequest(index_id=remote.index_id)).generation_id
        == new.generation_id
    )
    passed("native rebuild invalidates direct cursors and generation fences")

    cursor = client.search(query)
    collection.drop_search_index("default")
    wait_for(
        "replicated deletion",
        lambda: all(
            index.index_id != remote.index_id
            for index in client.list_indexes(api.ListIndexesRequest()).indexes
        ),
    )
    expect_status(
        grpc.StatusCode.NOT_FOUND,
        lambda: client.get_more(api.GetMoreRequest(cursor_id=cursor.cursor_id)),
    )
    expect_status(
        grpc.StatusCode.NOT_FOUND,
        lambda: client.get_index(api.GetIndexRequest(index_id=remote.index_id)),
    )
    assert client.get_index(api.GetIndexRequest(index_id=push.index_id)).queryable
    passed("native drop invalidates direct cursors without affecting standalone indexes")


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix=".all-index-reads-", dir=ROOT))
    scratch.chmod(0o755)
    suffix = secrets.token_hex(6)
    source, search = f"direct-read-mongo-{suffix}", f"direct-read-mongot-{suffix}"
    volumes = [f"direct-read-data-{suffix}", f"direct-read-indexes-{suffix}"]
    containers = []
    report = {"passed": False, "checks": []}
    source_port, search_port = free_port(), free_port()
    source_address, search_address = f"127.0.0.1:{source_port}", f"127.0.0.1:{search_port}"

    def passed(name):
        report["checks"].append(name)
        print("PASS:", name, flush=True)

    try:
        password = secrets.token_hex(24)
        (scratch / "password").write_text(password)
        (scratch / "password").chmod(0o400)
        (scratch / "mongod.conf").write_text(
            MONGOD_CONFIG.replace("port: 27017", f"port: {source_port}").replace(
                "mongot-community.search-community:27028", search_address
            )
        )
        (scratch / "mongot.conf").write_text(
            MONGOT_CONFIG.replace("mongod.search-community:27017", source_address)
            .replace("mongot-community.search-community:27028", search_address)
            .replace("mongot-community.search-community:9946", f"127.0.0.1:{free_port()}")
            .replace("mongot-community.search-community:8080", f"127.0.0.1:{free_port()}")
        )
        for volume in volumes:
            docker("volume", "create", volume)
        containers.append(source)
        docker(
            "run",
            "-d",
            "--name",
            source,
            "--network",
            "host",
            "--memory",
            "1g",
            "-v",
            f"{volumes[0]}:/data/db",
            "-v",
            f"{scratch / 'mongod.conf'}:/mongod.conf:ro",
            args.mongodb_image,
            "mongod",
            "--config",
            "/mongod.conf",
            "--wiredTigerCacheSizeGB",
            "0.25",
        )
        with MongoClient(
            source_address, directConnection=True, serverSelectionTimeoutMS=1000
        ) as mongo:
            wait_for("MongoDB", lambda: mongo.admin.command("ping"))
            report["mongodb_version"] = mongo.admin.command("buildInfo")["version"]
            mongo.admin.command(
                "replSetInitiate", {"_id": "rs0", "members": [{"_id": 0, "host": source_address}]}
            )
            wait_for("primary", lambda: mongo.admin.command("hello").get("isWritablePrimary"))
            mongo.admin.command(
                "createUser", "mongotUser", pwd=password, roles=["searchCoordinator"]
            )
            containers.append(search)
            docker(
                "run",
                "-d",
                "--name",
                search,
                "--network",
                "host",
                "--user",
                "0:0",
                "--memory",
                "2g",
                "-e",
                "JAVA_TOOL_OPTIONS=-Xms128m -Xmx768m -XX:ActiveProcessorCount=2",
                "-v",
                f"{args.package}:/opt/mongot:ro",
                "-v",
                f"{volumes[1]}:/data/mongot",
                "-v",
                f"{scratch / 'mongot.conf'}:/mongot.conf:ro",
                "-v",
                f"{scratch / 'password'}:/passwordFile:ro",
                "--entrypoint",
                "/opt/mongot/mongot",
                args.runtime_image,
                "--config",
                "/mongot.conf",
            )
            with Client(search_address, timeout=30) as client:
                wait_for(
                    "direct service",
                    lambda: client.get_status(api.GetStatusRequest()).management_ready,
                )
                exercise(client, mongo, passed, source, search)
        report["passed"] = True
    finally:
        for container in reversed(containers):
            (args.output / f"{container}.log").write_text(docker("logs", container, check=False))
            docker("rm", "--force", "--volumes", container, check=False)
        for volume in volumes:
            docker("volume", "rm", volume, check=False)
        shutil.rmtree(scratch)
        (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--mongodb-image", default="mongo:8.3.17-noble")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.package = args.package.resolve()
    args.output = args.output.resolve()
    run(args)
