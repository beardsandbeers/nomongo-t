"""Live coexistence regression test; requires pymongo, grpcio, PyYAML and the Python client.

Build and extract //deploy:mongot-standalone first. Pass its mongot launcher, an existing
mongod binary, and a scratch directory on an appropriate data disk. All database files and
logs are kept under that directory.
"""

import argparse
import contextlib
import os
from pathlib import Path
import socket
import struct
import subprocess
import time

from bson import json_util
import grpc
import pymongo
import yaml

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
            self.process = subprocess.Popen(
                self.args, stdout=log, stderr=log, env=self.env
            )
        return self

    def check(self):
        if self.process is not None and self.process.poll() is not None:
            raise RuntimeError(
                f"Process exited with {self.process.returncode}: {self.logfile}"
            )

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
        call = legacy(
            iter([document]), metadata=(("mongodb-wireversion", "25"),), timeout=60
        )
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
            index if index.active_revision == revision and index.generation_id else None
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
        wait_for(
            "primary", lambda: mongo.admin.command("hello").get("isWritablePrimary")
        )
        mongo.admin.command(
            "createUser", "test", pwd=password.read_text(), roles=["root", "__system"]
        )
        collection.insert_one({"_id": "remote-one", "title": "replicated original"})
        collection_uuid = mongo.coexistence.command(
            "listCollections", filter={"name": "records"}
        )["cursor"]["firstBatch"][0]["info"]["uuid"]
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
        before = json_util.loads((search_data / "configJournal.json").read_text())[
            "indexes"
        ]
        assert len(before) == 1
        remote_id = str(before[0]["index"]["indexID"])
        remote_generation = before[0]["generation"]

        print("Enabling coexistence on the same replicated data directory", flush=True)
        config["standalone"] = {"enabled": True, "grpc": {"listener": "shared"}}
        config_file.write_text(yaml.safe_dump(config))
        mongot.start()
        wait_for(
            "standalone management",
            lambda: client.get_status(api.GetStatusRequest()).management_ready,
            process=mongot,
        )
        assert not client.list_indexes(api.ListIndexesRequest()).indexes
        wait_for("recovered native content", lambda: native_results("original"))
        recovered = json_util.loads((search_data / "configJournal.json").read_text())[
            "indexes"
        ]
        assert recovered[0]["generation"] == remote_generation
        assert str(recovered[0]["index"]["indexID"]) == remote_id
        local = client.create_index(
            api.CreateIndexRequest(
                name="shared-name",
                type=api.INDEX_TYPE_SEARCH,
                definition_bson=encode_bson({"mappings": {"dynamic": True}}),
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
        client.upsert(
            local.index_id, {"_id": "local-two", "title": "manual concurrent"}
        )
        wait_for("replicated update", lambda: native_results("concurrent"))
        assert len(local_results(local.index_id, "concurrent")) == 1
        assert not native_results("manual")
        assert not local_results(local.index_id, "replicated")
        listed = native_wire(
            dict(management_base, userCommand={"listSearchIndexes": "records"})
        )
        assert len(listed["cursor"]["firstBatch"]) == 1
        try:
            client.get_index(api.GetIndexRequest(index_id=remote_id))
            raise AssertionError("Standalone catalog exposed a replicated index")
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
        temporary = client.create_index(
            api.CreateIndexRequest(
                name="temporary",
                type=api.INDEX_TYPE_SEARCH,
                definition_bson=encode_bson({"mappings": {"dynamic": True}}),
            )
        )
        wait_for("offline local index", lambda: local_ready(temporary.index_id))
        client.drop_index(
            api.DropIndexRequest(index_id=temporary.index_id, expected_revision=1)
        )
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
        wait_for("offline recovered index", lambda: local_ready(local.index_id))
        assert len(local_results(local.index_id, "offline")) == 1
        mongod.start()
        wait_for("source recovery", lambda: native_results("concurrent"))
        collection.insert_one({"_id": "remote-three", "title": "replicated resumed"})
        wait_for("resumed replication", lambda: native_results("resumed"))
        assert len(client.list_indexes(api.ListIndexesRequest()).indexes) == 1
        native_wire(
            dict(
                management_base,
                userCommand={"dropSearchIndex": "records", "name": "shared-name"},
            )
        )
        assert len(local_results(local.index_id, "offline")) == 1
        client.drop_index(
            api.DropIndexRequest(index_id=local.index_id, expected_revision=1)
        )
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
