"""Live coexistence regression test; requires pymongo, grpcio, PyYAML and the Python client.

Build and extract //deploy:mongot-standalone first. Pass its mongot launcher, an existing
mongod binary, and a scratch directory on an appropriate data disk. All database files and
logs are kept under that directory.
"""

import argparse
import contextlib
import hashlib
import os
import socket
import struct
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
import pymongo
import yaml
from bson import json_util
from mongot_standalone import Client, api, decode_bson, encode_bson


def wait_for(description, action, timeout=180, process=None):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        if process is not None:
            process.check()
        try:
            result = action()
            if result:
                return result
        except (AssertionError, grpc.RpcError, pymongo.errors.PyMongoError) as error:
            last_error = error
        time.sleep(0.5)
    raise AssertionError(f"Timed out waiting for {description}: {last_error}")


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class Process:
    def __init__(self, args, logfile, env=None):
        self.args, self.logfile, self.env = args, logfile, env
        self.process = None

    def start(self):
        with self.logfile.open("ab") as log:
            self.process = subprocess.Popen(self.args, stdout=log, stderr=log, env=self.env)
        return self

    def check(self):
        if self.process is not None and self.process.poll() is not None:
            raise RuntimeError(f"Process exited with {self.process.returncode}: {self.logfile}")

    def stop(self, verify=True):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=90)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
                if verify:
                    raise AssertionError(f"Process did not drain: {self.logfile}")
        if verify and self.process is not None:
            assert self.process.returncode in (
                0,
                143,
            ), f"Process failed: {self.logfile}"


def run(args):
    directory = args.workdir.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    mongo_data, search_data = directory / "mongo", directory / "search"
    mongo_data.mkdir()
    search_data.mkdir()
    mongo_port, grpc_port, health_port, metrics_port = port(), port(), port(), port()
    address = f"127.0.0.1:{grpc_port}"
    mongo = pymongo.MongoClient(
        f"mongodb://127.0.0.1:{mongo_port}/?directConnection=true",
        serverSelectionTimeoutMS=1000,
        socketTimeoutMS=60000,
    )
    mongod = Process(
        [
            str(args.mongod.resolve()),
            "--replSet",
            "rs0",
            "--bind_ip",
            "127.0.0.1",
            "--port",
            str(mongo_port),
            "--dbpath",
            str(mongo_data),
            "--wiredTigerCacheSizeGB",
            "0.25",
        ],
        directory / "mongod.log",
    )
    password = directory / "password"
    password.write_text("coexistence-test-password")
    password.chmod(0o400)
    config = {
        "syncSource": {
            "replicaSet": {
                "hostAndPort": f"127.0.0.1:{mongo_port}",
                "scramAuth": {
                    "username": "test",
                    "authSource": "admin",
                    "passwordFile": str(password),
                    "tls": {"enabled": False},
                },
            }
        },
        "storage": {"dataPath": str(search_data)},
        "server": {"grpc": {"address": address}},
        "healthCheck": {"address": f"127.0.0.1:{health_port}"},
        "metrics": {"enabled": False, "address": f"127.0.0.1:{metrics_port}"},
    }
    config_file = directory / "mongot.yaml"
    config_file.write_text(yaml.safe_dump(config))
    temporary = directory / "tmp"
    temporary.mkdir()
    env = dict(
        os.environ,
        JAVA_TOOL_OPTIONS=(
            f"-Xms128m -Xmx768m -XX:MaxDirectMemorySize=256m --enable-preview -Djava.io.tmpdir={temporary}"
        ),
    )
    mongot = Process(
        [str(args.mongot.resolve()), "--config", str(config_file)],
        directory / "mongot.log",
        env,
    )
    client = Client(address, timeout=60)
    native_manage = client.channel.unary_unary(
        "/mongodb.BsonCommandService/manageSearchIndex",
        request_serializer=encode_bson,
        response_deserializer=decode_bson,
    )

    def encode_wire(document):
        body = encode_bson(document)
        return struct.pack("<iiiiiB", 21 + len(body), 1, 0, 2013, 0, 0) + body

    def decode_wire(message):
        length, _, response_to, opcode = struct.unpack("<iiii", message[:16])
        assert length == len(message) and response_to == 1 and opcode == 2013
        assert message[20] == 0
        size = struct.unpack("<i", message[21:25])[0]
        return decode_bson(message[21 : 21 + size])

    legacy = client.channel.stream_stream(
        "/mongodb.CommandService/UnauthenticatedCommandStream",
        request_serializer=encode_wire,
        response_deserializer=decode_wire,
    )

    def native_wire(document):
        call = legacy(iter([document]), metadata=(("mongodb-wireversion", "25"),), timeout=60)
        try:
            response = next(call)
            assert dict(call.initial_metadata())["mongodb-maxwireversion"] == "26"
            assert response.get("ok"), response
            return response
        finally:
            call.cancel()

    collection = mongo.coexistence.records

    def native_results(term):
        response = native_wire(
            {
                "search": "records",
                "$db": "coexistence",
                "collectionUUID": collection_uuid,
                "query": {
                    "index": "shared-name",
                    "text": {"query": term, "path": "title"},
                },
                "cursorOptions": {"batchSize": 100},
            }
        )
        return response["cursor"]["nextBatch"]

    def local_ready(index_id, revision=1):
        index = client.get_index(api.GetIndexRequest(index_id=index_id))
        return (
            index
            if index.queryable
            and index.lifecycle_state == "READY"
            and index.active_revision == revision
            and index.generation_id
            else None
        )

    def local_results(index_id, term):
        response = client.search(
            api.QueryRequest(
                index_id=index_id,
                query_bson=encode_bson(
                    {"index": "shared-name", "text": {"query": term, "path": "title"}}
                ),
                batch_size=100,
            )
        )
        return decode_bson(response.batch_bson)["results"]

    success = False
    try:
        print("Starting MongoDB fixture", flush=True)
        mongod.start()
        wait_for("mongod", lambda: mongo.admin.command("ping"), process=mongod)
        mongo.admin.command(
            "replSetInitiate",
            {"_id": "rs0", "members": [{"_id": 0, "host": f"127.0.0.1:{mongo_port}"}]},
        )
        wait_for("primary", lambda: mongo.admin.command("hello").get("isWritablePrimary"))
        mongo.admin.command(
            "createUser", "test", pwd=password.read_text(), roles=["root", "__system"]
        )
        collection.insert_one({"_id": "remote-one", "title": "replicated original"})
        collection_uuid = mongo.coexistence.command("listCollections", filter={"name": "records"})[
            "cursor"
        ]["firstBatch"][0]["info"]["uuid"]
        management_base = {
            "manageSearchIndex": "records",
            "$db": "coexistence",
            "collectionUUID": collection_uuid,
        }
        print("Creating and querying through native gRPC (wire version 25)", flush=True)
        mongot.start()
        wait_for(
            "native management",
            lambda: native_manage(
                dict(management_base, userCommand={"listSearchIndexes": "records"}),
                timeout=5,
            ).get("ok"),
            process=mongot,
        )
        native_wire(
            dict(
                management_base,
                userCommand={
                    "createSearchIndexes": "records",
                    "indexes": [
                        {
                            "name": "shared-name",
                            "definition": {"mappings": {"dynamic": True}},
                        }
                    ],
                },
            )
        )
        wait_for("initial replicated content", lambda: native_results("original"))
        mongot.stop()
        before = json_util.loads((search_data / "configJournal.json").read_text())["indexes"]
        assert len(before) == 1
        remote_id = str(before[0]["index"]["indexID"])
        remote_generation = before[0]["generation"]

        print("Restarting automatic dual ingestion on the same data directory", flush=True)
        config_file.write_text(yaml.safe_dump(config))
        mongot.start()
        wait_for(
            "standalone management",
            lambda: client.get_status(api.GetStatusRequest()).management_ready,
            process=mongot,
        )
        assert not client.list_indexes(
            api.ListIndexesRequest(ownership=api.INDEX_OWNERSHIP_STANDALONE)
        ).indexes
        wait_for("recovered native content", lambda: native_results("original"))
        replicated = client.list_indexes(
            api.ListIndexesRequest(ownership=api.INDEX_OWNERSHIP_REPLICATED)
        ).indexes
        assert [index.index_id for index in replicated] == [remote_id]
        recovered = json_util.loads((search_data / "configJournal.json").read_text())["indexes"]
        assert recovered[0]["generation"] == remote_generation
        assert str(recovered[0]["index"]["indexID"]) == remote_id
        local = client.create_index(
            api.CreateIndexRequest(
                name="shared-name",
                type=api.INDEX_TYPE_SEARCH,
                definition_bson=encode_bson(
                    {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}}}}
                ),
            )
        )
        wait_for("local index", lambda: local_ready(local.index_id))
        client.upsert(
            local.index_id,
            {"_id": "local-one", "title": "manual original"},
            concern="committed",
        )
        wait_for("local visibility", lambda: local_results(local.index_id, "manual"))
        collection.insert_one({"_id": "remote-two", "title": "replicated concurrent"})
        client.upsert(local.index_id, {"_id": "local-two", "title": "manual concurrent"})
        wait_for("replicated update", lambda: native_results("concurrent"))
        wait_for(
            "concurrent local refresh",
            lambda: len(local_results(local.index_id, "concurrent")) == 1,
        )
        print("Deleting every local match while MongoDB replication continues", flush=True)
        for start in range(0, 10001, 1000):
            client.upsert_many(
                local.index_id,
                [
                    {"_id": f"purge-{i}", "title": "purge"}
                    for i in range(start, min(start + 1000, 10001))
                ],
            )
        with ThreadPoolExecutor(max_workers=1) as executor:
            deletion = executor.submit(
                client.delete_by_query,
                api.DeleteByQueryRequest(
                    index_id=local.index_id,
                    search_query_bson=encode_bson({"text": {"path": "title", "query": "purge"}}),
                ),
            )
            collection.insert_one(
                {"_id": "remote-during-delete", "title": "replicated duringpurge"}
            )
            deleted = deletion.result(timeout=60)
        assert deleted.matched_count == 10001
        assert deleted.processed_count == 10001
        assert not local_results(local.index_id, "purge")
        assert len(local_results(local.index_id, "manual")) == 2
        wait_for("replication during local deletion", lambda: native_results("duringpurge"))
        assert not native_results("manual")
        assert not local_results(local.index_id, "replicated")
        listed = native_wire(dict(management_base, userCommand={"listSearchIndexes": "records"}))
        assert len(listed["cursor"]["firstBatch"]) == 1
        remote = client.get_index(api.GetIndexRequest(index_id=remote_id))
        assert remote.ownership == api.INDEX_OWNERSHIP_REPLICATED
        assert local_results(remote_id, "original")
        try:
            client.upsert(remote_id, {"_id": "forbidden", "title": "manual"})
            raise AssertionError("Direct writes accepted a replicated index")
        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.NOT_FOUND

        print(
            "Checking local ingestion and management during a MongoDB outage",
            flush=True,
        )
        mongod.stop()
        client.upsert(
            local.index_id,
            {"_id": "offline", "title": "manual offline"},
            concern="committed",
        )
        wait_for(
            "committed local write visibility",
            lambda: local_results(local.index_id, "offline"),
        )
        print("Preserving a local schema update while MongoDB is unavailable", flush=True)
        previous_generation = client.get_index(
            api.GetIndexRequest(index_id=local.index_id)
        ).generation_id
        client.update_index(
            api.UpdateIndexRequest(
                index_id=local.index_id,
                expected_revision=1,
                update_mode=api.INDEX_UPDATE_MODE_REQUIRE_PRESERVE,
                definition_bson=encode_bson(
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {"title": {"type": "string"}, "category": {"type": "token"}},
                        }
                    }
                ),
            )
        )
        changed = wait_for("preserved local update", lambda: local_ready(local.index_id, 2))
        assert changed.generation_id == previous_generation
        assert len(local_results(local.index_id, "offline")) == 1
        client.upsert(
            local.index_id,
            {"_id": "offline", "title": "manual offline", "category": "local"},
            concern="committed",
        )
        wait_for(
            "added field visibility",
            lambda: (
                len(
                    decode_bson(
                        client.search(
                            api.QueryRequest(
                                index_id=local.index_id,
                                query_bson=encode_bson(
                                    {"equals": {"path": "category", "value": "local"}}
                                ),
                            )
                        ).batch_bson
                    )["results"]
                )
                == 1
            ),
        )
        temporary = client.create_index(
            api.CreateIndexRequest(
                name="temporary",
                type=api.INDEX_TYPE_SEARCH,
                definition_bson=encode_bson({"mappings": {"dynamic": True}}),
            )
        )
        wait_for("offline local index", lambda: local_ready(temporary.index_id))
        client.drop_index(api.DropIndexRequest(index_id=temporary.index_id, expected_revision=1))
        mongot.stop()

        print(
            "Restarting with MongoDB unavailable; recovering both ownership sets",
            flush=True,
        )
        mongot.start()
        wait_for(
            "offline startup management",
            lambda: client.get_status(api.GetStatusRequest()).management_ready,
            process=mongot,
        )
        wait_for("offline recovered index", lambda: local_ready(local.index_id, 2))
        assert len(local_results(local.index_id, "offline")) == 1
        mongod.start()
        wait_for("source recovery", lambda: native_results("concurrent"))
        collection.insert_one({"_id": "remote-three", "title": "replicated resumed"})
        wait_for("resumed replication", lambda: native_results("resumed"))

        print(
            "Checking local bootstrap recovery and data failure isolation with replication",
            flush=True,
        )
        probes = {}
        for name in ("interrupted-bootstrap", "damaged-ready"):
            probe = client.create_index(
                api.CreateIndexRequest(
                    name=name,
                    type=api.INDEX_TYPE_SEARCH,
                    definition_bson=encode_bson(
                        {"numPartitions": 2, "mappings": {"fields": {"title": {"type": "string"}}}}
                    ),
                )
            )
            wait_for(name, lambda index_id=probe.index_id: local_ready(index_id))
            probes[name] = probe.index_id
        client.upsert(
            probes["damaged-ready"], {"_id": "kept", "title": "retained"}, concern="committed"
        )
        mongot.stop()
        catalog_file = search_data / "indexes.json"
        catalog = json_util.loads(catalog_file.read_text())
        assert catalog["catalogVersion"] == 4
        bootstrap = next(
            entry
            for entry in catalog["initializations"]
            if str(entry["definition"]["indexID"]) == probes["interrupted-bootstrap"]
        )
        bootstrap["state"] = "INITIALIZING"
        catalog_file.write_text(json_util.dumps(catalog))
        probe_paths = {}
        for name, index_id in probes.items():
            paths = [path for path in search_data.glob(index_id + "*") if path.is_dir()]
            assert len(paths) == 1, paths
            probe_paths[name] = paths[0]
            next((paths[0] / "0").glob("segments_*")).unlink()
        mongot.start()
        wait_for(
            "resumed authorized bootstrap", lambda: local_ready(probes["interrupted-bootstrap"])
        )
        wait_for(
            "isolated missing local commit",
            lambda: (
                client.get_index(
                    api.GetIndexRequest(index_id=probes["damaged-ready"])
                ).lifecycle_state
                == "FAILED"
            ),
        )
        assert all(
            list((probe_paths["interrupted-bootstrap"] / str(part)).glob("segments_*"))
            for part in (0, 1)
        )
        assert not list((probe_paths["damaged-ready"] / "0").glob("segments_*"))
        assert client.get_status(api.GetStatusRequest()).management_ready
        wait_for("replication after isolated local failure", lambda: native_results("resumed"))
        assert len(local_results(local.index_id, "offline")) == 1
        for index_id in probes.values():
            client.drop_index(api.DropIndexRequest(index_id=index_id, expected_revision=1))
        assert (
            len(
                client.list_indexes(
                    api.ListIndexesRequest(ownership=api.INDEX_OWNERSHIP_STANDALONE)
                ).indexes
            )
            == 1
        )

        print("Checking recovery of missing local metadata on a mixed volume", flush=True)
        mongot.stop()
        journal_file = search_data / "configJournal.json"
        original_journal = journal_file.read_bytes()
        ownership_file = search_data / "standalone-owned-ids.json"

        def local_data_hashes():
            files = [journal_file, ownership_file]
            if catalog_file.exists():
                files.append(catalog_file)
            for path in search_data.glob(local.index_id + "*"):
                if path.is_dir():
                    files.extend(file for file in path.rglob("*") if file.is_file())
            return {
                str(file.relative_to(search_data)): hashlib.sha256(file.read_bytes()).hexdigest()
                for file in files
            }

        saved_catalog = catalog_file.rename(directory / "saved-indexes.json")
        try:
            before_rejection = local_data_hashes()
            mongot.start()
            assert mongot.process.wait(timeout=90) != 0
            assert "Missing indexes.json" in mongot.logfile.read_text()
            assert local_data_hashes() == before_rejection
        finally:
            saved_catalog.rename(catalog_file)

        print("Rejecting unreadable local journal metadata before native cleanup", flush=True)
        journal = json_util.loads(original_journal)
        local_entry = next(
            entry
            for entry in journal["indexes"]
            if str(entry["index"]["indexID"]) == local.index_id
        )
        local_entry["futureProperty"] = True
        journal_file.write_text(json_util.dumps(journal))

        before_rejection = local_data_hashes()
        mongot.start()
        assert mongot.process.wait(timeout=60) != 0
        assert "Unreadable local index journal entry" in mongot.logfile.read_text()
        assert local_data_hashes() == before_rejection
        journal_file.write_bytes(original_journal)
        mongot.start()
        wait_for(
            "local data after journal repair",
            lambda: local_ready(local.index_id, 2),
            process=mongot,
        )
        assert len(local_results(local.index_id, "offline")) == 1
        wait_for("replication after journal repair", lambda: native_results("resumed"))
        native_wire(
            dict(
                management_base,
                userCommand={"dropSearchIndex": "records", "name": "shared-name"},
            )
        )
        assert len(local_results(local.index_id, "offline")) == 1
        client.drop_index(api.DropIndexRequest(index_id=local.index_id, expected_revision=2))
        success = True
        print(
            "PASS: native commands, existing-data adoption, source isolation, writes, outages and restart",
            flush=True,
        )
    finally:
        client.close()
        with contextlib.ExitStack() as cleanup:
            cleanup.callback(mongo.close)
            cleanup.callback(mongod.stop, verify=success)
            cleanup.callback(mongot.stop, verify=success)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongod", type=Path, required=True)
    parser.add_argument("--mongot", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
