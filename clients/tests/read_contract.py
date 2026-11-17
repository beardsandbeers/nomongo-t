#!/usr/bin/env python3
"""Verify contract rejection and cursor cleanup through all four built client CLIs.

Run with clients/python/.venv/bin/python clients/tests/read_contract.py after building
the Java, Node, and Rust clients. Uses a local gRPC fixture; no mongot process is needed.
"""

import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
from e2e import Driver, encoded
from mongot_standalone import api
from mongot_standalone import standalone_pb2_grpc as rpc

CONTRACT_KEY = "mongot-direct-read-contract"
CONTRACT = "all-index-reads-v1"
CURSOR_ID = (1 << 60) + 7  # Detect accidental loss of precision in the Node client.


class Server(rpc.QueryServiceServicer):
    def __init__(self):
        self.lock = threading.Lock()

    def configure(self, acknowledgement, cursor, fail_cleanup):
        with self.lock:
            self.acknowledgement = acknowledgement
            self.cursor = cursor
            self.fail_cleanup = fail_cleanup
            self.kills = []
            self.open_cursors = set()
            self.search_headers = []
            self.cleanup_headers = []
            self.cleanup_deadlines = []

    def Search(self, request, context):
        with self.lock:
            self.search_headers = list(context.invocation_metadata())
            if self.acknowledgement:
                context.send_initial_metadata(((CONTRACT_KEY, self.acknowledgement),))
            if self.cursor:
                self.open_cursors.add(self.cursor)
            return api.QueryResponse(cursor_id=self.cursor)

    def VectorSearch(self, request, context):
        return self.Search(request, context)

    def KillCursor(self, request, context):
        with self.lock:
            self.kills.append(request.cursor_id)
            self.cleanup_headers = list(context.invocation_metadata())
            self.cleanup_deadlines.append(context.time_remaining())
            if self.fail_cleanup:
                context.abort(grpc.StatusCode.UNAVAILABLE, "Cleanup deliberately unavailable")
            self.open_cursors.discard(request.cursor_id)
        return api.KillCursorResponse()


class ReadContractTest(unittest.TestCase):
    def test_contract_rejection_cleans_up_discarded_cursors(self):
        fixture = Server()
        server = grpc.server(ThreadPoolExecutor(max_workers=2))
        rpc.add_QueryServiceServicer_to_server(fixture, server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        environment = dict(os.environ, MONGOT_ADDRESS=f"127.0.0.1:{port}", MONGOT_TIMEOUT="10")
        environment.pop("MONGOT_CA", None)
        environment.pop("MONGOT_TOKEN_FILE", None)
        request = {
            "indexId": "0123456789abcdef01234567",
            "queryBson": encoded({"exists": {"path": "key"}}),
            "joinIndexes": [{"alias": "inventory", "indexId": "1123456789abcdef01234567"}],
        }
        scenarios = [
            ("missing acknowledgement", None, CURSOR_ID, False),
            ("wrong acknowledgement", "other-contract", CURSOR_ID, False),
            ("cleanup failure", None, CURSOR_ID, True),
            ("exhausted cursor", None, 0, False),
            ("accepted response", CONTRACT, CURSOR_ID, False),
        ]
        try:
            with tempfile.TemporaryDirectory(prefix="mongot-read-contract-") as logs:
                for language in ("python", "java", "node", "rust"):
                    with Driver(language, environment, Path(logs)) as driver:
                        for method in ("Search", "VectorSearch"):
                            for name, acknowledgement, cursor, fail_cleanup in scenarios:
                                with self.subTest(language=language, method=method, scenario=name):
                                    fixture.configure(acknowledgement, cursor, fail_cleanup)
                                    if acknowledgement == CONTRACT:
                                        response = driver.call(method, request)
                                        self.assertEqual(int(response["cursorId"]), cursor)
                                    else:
                                        code = (
                                            "CLIENT_ERROR"
                                            if language in ("python", "node")
                                            else "FAILED_PRECONDITION"
                                        )
                                        error = driver.call(method, request, error=code)
                                        self.assertIn(
                                            "does not support the all-index direct read contract",
                                            error["message"],
                                        )
                                    with fixture.lock:
                                        self.assertIn(
                                            (CONTRACT_KEY, CONTRACT), fixture.search_headers
                                        )
                                        should_kill = cursor != 0 and acknowledgement != CONTRACT
                                        self.assertEqual(
                                            fixture.kills, [cursor] if should_kill else []
                                        )
                                        self.assertNotIn(
                                            CONTRACT_KEY, dict(fixture.cleanup_headers)
                                        )
                                        if should_kill:
                                            self.assertEqual(len(fixture.cleanup_deadlines), 1)
                                            self.assertGreater(fixture.cleanup_deadlines[0], 0)
                                            # Allow gRPC's timeout encoding to round up slightly.
                                            self.assertLessEqual(fixture.cleanup_deadlines[0], 5.1)
                                        remaining = (
                                            {cursor}
                                            if cursor
                                            and (fail_cleanup or acknowledgement == CONTRACT)
                                            else set()
                                        )
                                        self.assertEqual(fixture.open_cursors, remaining)
        finally:
            server.stop(0).wait()


if __name__ == "__main__":
    unittest.main()
