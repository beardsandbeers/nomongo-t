"""Swap official mongot for a candidate image without changing config or index data.

Requires Docker, pymongo, grpcio, and the repository's Python client. The defaults follow
https://www.mongodb.com/docs/search/self-managed/current/installation/docker/
Image digests and versions are recorded so failures can be reproduced with pinned images.
Only resources created by this run are removed. Logs/results remain in --output.
"""

import argparse
import hashlib
import json
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
from bson import json_util
from mongot_standalone import Client, api
from pymongo import MongoClient
from pymongo.errors import PyMongoError

MONGOD_CONFIG = """net:
  port: 27017
  bindIpAll: true
replication:
  replSetName: rs0
setParameter:
  searchIndexManagementHostAndPort: mongot-community.search-community:27028
  mongotHost: mongot-community.search-community:27028
  skipAuthenticationToSearchIndexManagementServer: false
  useGrpcForSearch: true
  searchTLSMode: disabled
"""

MONGOT_CONFIG = """syncSource:
  replicaSet:
    hostAndPort: "mongod.search-community:27017"
    scramAuth:
      username: "mongotUser"
      passwordFile: "/passwordFile"
      authSource: "admin"
      tls:
        enabled: false
  replicationReader:
    readPreference: "secondaryPreferred"
storage:
  dataPath: "/data/mongot"
server:
  grpc:
    address: "mongot-community.search-community:27028"
    tls:
      mode: "disabled"
metrics:
  enabled: true
  address: "mongot-community.search-community:9946"
healthCheck:
  address: "mongot-community.search-community:8080"
logging:
  verbosity: INFO
"""


def docker(*args, check=True, timeout=180):
    result = subprocess.run(
        ["docker", *map(str, args)], check=False, capture_output=True, text=True, timeout=timeout
    )
    if check and result.returncode:
        raise RuntimeError(f"docker {args[0]} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def wait_for(description, action, predicate=bool, timeout=180):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            last = action()
            if predicate(last):
                return last
        except (PyMongoError, grpc.RpcError, OSError) as error:
            last = str(error)
        time.sleep(0.5)
    raise AssertionError(f"{description} timed out: {last}")


def mapped_port(container, port):
    return int(docker("port", container, f"{port}/tcp").rsplit(":", 1)[1])


def image_info(image):
    data = json.loads(docker("image", "inspect", image))[0]
    return {"reference": image, "id": data["Id"], "digests": data.get("RepoDigests", [])}


def run(args):
    args.output.mkdir(parents=True, exist_ok=True)
    suffix = secrets.token_hex(5)
    network, source, search = (
        f"mongot-compat-{suffix}-{part}" for part in ["net", "mongo", "search"]
    )
    volumes = [f"mongot-compat-{suffix}-{part}" for part in ["database", "configdb", "indexes"]]
    # Docker must see these bind mounts in its host namespace; avoid private /tmp mounts.
    scratch = Path(tempfile.mkdtemp(prefix=".mongot-compat-", dir=Path.cwd())).resolve()
    scratch.chmod(0o755)
    results = {"checks": [], "images": {}, "passed": False}
    mongo = None
    stage = "setup"
    created_volumes, created_containers, pulled_images = [], [], []
    created_network = False

    def passed(name, **details):
        results["checks"].append({"name": name, **details})
        print(f"PASS: {name}", flush=True)

    def save_logs(name):
        output = subprocess.run(
            ["docker", "logs", name], check=False, capture_output=True, text=True, timeout=30
        )
        (args.output / f"{stage}-{name}.log").write_text(output.stdout + output.stderr)

    def start_search(image):
        # The image is the ONLY changed Docker argument across the swap.
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
            "--memory",
            "2g",
            "--cpus",
            "2",
            "-e",
            "JAVA_TOOL_OPTIONS=-Xms128m -Xmx768m -XX:ActiveProcessorCount=2",
            "-v",
            f"{volumes[2]}:/data/mongot",
            "-v",
            f"{scratch / 'mongot.conf'}:/mongot-community/config.default.yml:ro",
            "-v",
            f"{scratch / 'passwordFile'}:/passwordFile:ro",
            "-p",
            "127.0.0.1::27028",
            "-p",
            "127.0.0.1::8080",
            "-p",
            "127.0.0.1::9946",
            image,
        )

    def query(term="coffee"):
        return sorted(
            doc["_id"]
            for doc in mongo.compat.records.aggregate(
                [
                    {"$search": {"index": "default", "text": {"path": "title", "query": term}}},
                    {"$project": {"_id": 1}},
                ],
                batchSize=2,
                maxTimeMS=10000,
            )
        )

    def journal(label):
        path = args.output / f"{label}-journal.json"
        docker("cp", f"{search}:/data/mongot/configJournal.json", path)
        return json_util.loads(path.read_text())

    def vectors():
        return [
            doc["_id"]
            for doc in mongo.compat.records.aggregate(
                [
                    {
                        "$vectorSearch": {
                            "index": "vectors",
                            "path": "embedding",
                            "queryVector": [1.0, 0.0, 0.0],
                            "exact": True,
                            "limit": 3,
                        }
                    },
                    {"$project": {"_id": 1}},
                ],
                maxTimeMS=10000,
            )
        ]

    def identity(value):
        entry = next(item for item in value["indexes"] if item["index"]["name"] == "default")
        vector = next(item for item in value["vectorIndexes"] if item["name"] == "vectors")
        return {
            "search": {
                "id": str(entry["index"]["indexID"]),
                "generation": entry["generation"],
                "collectionUUID": str(entry["index"]["collectionUUID"]),
            },
            "vector": {
                "id": str(vector["indexID"]),
                "generation": vector["generation"],
                "collectionUUID": str(vector["collectionUUID"]),
            },
        }

    def health():
        with urllib.request.urlopen(
            f"http://127.0.0.1:{mapped_port(search, 8080)}/health", timeout=5
        ) as response:
            value = response.read().decode().strip()
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return value == "SERVING"
            return (
                value.get("status") == "SERVING" if isinstance(value, dict) else value == "SERVING"
            )

    try:
        results["images"]["candidate"] = image_info(args.candidate_image)
        for kind, image in [("reference", args.reference_image), ("mongodb", args.mongodb_image)]:
            existed = bool(docker("image", "ls", "-q", image))
            if not args.skip_pull:
                docker("pull", image, timeout=300)
                if not existed:
                    pulled_images.append(image)
            results["images"][kind] = image_info(image)
        (scratch / "mongod.conf").write_text(MONGOD_CONFIG)
        (scratch / "mongot.conf").write_text(MONGOT_CONFIG)
        password = secrets.token_hex(24)
        (scratch / "passwordFile").write_text(password)
        (scratch / "passwordFile").chmod(0o400)
        config_hash = hashlib.sha256((scratch / "mongot.conf").read_bytes()).hexdigest()
        results["configuration_sha256"] = config_hash
        (args.output / "mongot.conf").write_text(MONGOT_CONFIG)
        (args.output / "mongod.conf").write_text(MONGOD_CONFIG)
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
            "2",
            "-v",
            f"{volumes[0]}:/data/db",
            "-v",
            f"{volumes[1]}:/data/configdb",
            "-v",
            f"{scratch / 'mongod.conf'}:/etc/mongod.conf:ro",
            "-p",
            "127.0.0.1::27017",
            args.mongodb_image,
            "--config",
            "/etc/mongod.conf",
            "--replSetMember=mongod.search-community:27017",
        )
        mongo = MongoClient(
            "127.0.0.1",
            mapped_port(source, 27017),
            directConnection=True,
            serverSelectionTimeoutMS=2000,
            socketTimeoutMS=15000,
        )
        wait_for(
            "replica-set primary", lambda: mongo.admin.command("hello").get("isWritablePrimary")
        )
        results["mongodb_version"] = mongo.admin.command("buildInfo")["version"]
        mongo.admin.command("createUser", "mongotUser", pwd=password, roles=["searchCoordinator"])
        mongo.compat.records.insert_many(
            [
                {"_id": i, "title": "coffee original", "embedding": [1.0, i / 10.0, 0.0]}
                for i in range(8)
            ]
        )
        stage = "reference"
        start_search(args.reference_image)
        wait_for("official health", health)
        results["reference_version"] = docker(
            "exec", search, "cat", "/mongot-community/VERSION.txt"
        )
        wait_for(
            "official index creation",
            lambda: mongo.compat.command(
                {
                    "createSearchIndexes": "records",
                    "indexes": [
                        {
                            "name": "default",
                            "definition": {"numPartitions": 2, "mappings": {"dynamic": True}},
                        }
                    ],
                }
            ),
        )
        wait_for("official indexed documents", query, lambda ids: ids == list(range(8)))
        passed("official image indexes and serves paginated $search")
        mongo.compat.command(
            {
                "createSearchIndexes": "records",
                "indexes": [
                    {
                        "name": "vectors",
                        "type": "vectorSearch",
                        "definition": {
                            "fields": [
                                {
                                    "type": "vector",
                                    "path": "embedding",
                                    "numDimensions": 3,
                                    "similarity": "cosine",
                                }
                            ]
                        },
                    }
                ],
            }
        )
        wait_for("official vector index", vectors, lambda ids: ids == [0, 1, 2])
        passed("official image serves exact $vectorSearch")
        docker("stop", "--timeout", "45", search, timeout=60)
        before = identity(journal("reference"))
        results["index_before"] = before
        save_logs(search)
        docker("rm", "-v", search)
        created_containers.remove(search)
        mongo.compat.records.insert_one({"_id": 8, "title": "coffee during swap"})

        stage = "candidate"
        start_search(args.candidate_image)
        wait_for("candidate health with unchanged upstream config", health)
        wait_for("recovered original index and catch-up", query, lambda ids: ids == list(range(9)))
        after = identity(journal("candidate"))
        assert after == before, (before, after)
        assert hashlib.sha256((scratch / "mongot.conf").read_bytes()).hexdigest() == config_hash
        results["index_after"] = after
        passed("image-only swap preserves configuration, collection UUID, index ID and generation")
        passed("replication catches up with writes made while mongot was stopped")
        wait_for("recovered vector index", vectors, lambda ids: ids == [0, 1, 2])
        passed("replacement recovers and queries the official vector index")
        with ThreadPoolExecutor(max_workers=3) as pool:
            operations = [
                pool.submit(mongo.compat.records.insert_one, {"_id": 9, "title": "coffee new"}),
                pool.submit(
                    mongo.compat.records.update_one, {"_id": 0}, {"$set": {"title": "tea updated"}}
                ),
                pool.submit(mongo.compat.records.delete_one, {"_id": 1}),
            ]
            for operation in operations:
                operation.result(timeout=30)
        wait_for("post-swap replication", query, lambda ids: ids == list(range(2, 10)))
        wait_for("post-swap vector deletion", vectors, lambda ids: ids == [0, 2, 3])
        explanation = mongo.compat.command(
            {
                "explain": {
                    "aggregate": "records",
                    "pipeline": [{"$search": {"text": {"path": "title", "query": "coffee"}}}],
                    "cursor": {},
                },
                "verbosity": "executionStats",
            }
        )
        assert "indexPartitionExplain" in str(explanation)
        passed("post-swap insert/update/delete replication, paging and explain")
        with Client(f"127.0.0.1:{mapped_port(search, 27028)}", timeout=10) as api_client:
            try:
                api_client.get_status(api.GetStatusRequest())
                raise AssertionError("Unconfigured remote standalone API was exposed")
            except grpc.RpcError as error:
                assert error.code() == grpc.StatusCode.UNAUTHENTICATED, error
        passed("added API is registered and denies unauthenticated remote plaintext requests")
        docker("stop", "--timeout", "45", search, timeout=60)
        save_logs(search)
        docker("start", search)
        wait_for("candidate restart", health)
        wait_for(
            "candidate restart preserves results", query, lambda ids: ids == list(range(2, 10))
        )
        assert identity(journal("restarted")) == before
        wait_for("restarted vector results", vectors, lambda ids: ids == [0, 2, 3])
        passed("candidate restart preserves existing index identity and results")
        results["passed"] = True
    except Exception as error:
        results["failure_stage"] = stage
        results["failure"] = str(error)
        raise
    finally:
        cleanup_errors = []

        def cleanup(action):
            try:
                action()
            except (OSError, RuntimeError, subprocess.SubprocessError, PyMongoError) as error:
                cleanup_errors.append(str(error))

        if mongo is not None:
            cleanup(mongo.close)
        for container in reversed(created_containers):
            if docker("container", "inspect", container, check=False):
                cleanup(lambda container=container: save_logs(container))
                cleanup(lambda container=container: docker("rm", "--force", "--volumes", container))
        for volume in created_volumes:
            cleanup(lambda volume=volume: docker("volume", "rm", volume))
        if created_network:
            cleanup(lambda: docker("network", "rm", network))
        for image in pulled_images:
            cleanup(lambda image=image: docker("image", "rm", image))
        cleanup(lambda: shutil.rmtree(scratch))
        results["cleanup_errors"] = cleanup_errors
        if cleanup_errors:
            results["passed"] = False
        (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(f"Results: {args.output / 'results.json'}", flush=True)
        if cleanup_errors:
            raise RuntimeError(f"Fixture cleanup failed: {cleanup_errors}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-image", required=True)
    parser.add_argument("--reference-image", default="mongodb/mongodb-community-search:latest")
    parser.add_argument("--mongodb-image", default="mongodb/mongodb-community-server:latest")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--skip-pull", action="store_true", help="Use preloaded reference images")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
