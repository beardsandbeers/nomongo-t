"""Bound index helpers preserve BSON identity and never invent durable request IDs."""

import unittest
from types import SimpleNamespace

import bson
from bson import Int64, ObjectId
from mongot_standalone import api
from mongot_standalone.documents import DocumentCommandsMixin


class RecordingClient(DocumentCommandsMixin):
    timeout = 30
    metadata = ()

    def __init__(self):
        self.calls = []

        def record(request, **options):
            self.calls.append((request, options))
            return None

        self.documents = SimpleNamespace(Upsert=record, Delete=record, DeleteByQuery=record)


class IndexHelpersTest(unittest.TestCase):
    def test_bound_helpers_target_one_index_and_preserve_bson(self):
        client = RecordingClient()
        oid = ObjectId()
        client.index("first").upsert({"_id": oid, "big": Int64(2**60 + 1)}, concern="committed")
        client.index("second").delete_many([oid, Int64(1), 1])
        upsert = client.calls[0][0]
        self.assertEqual(upsert.index_id, "first")
        self.assertEqual(bson.decode(upsert.documents_bson[0])["big"], 2**60 + 1)
        self.assertEqual(upsert.write_concern, api.DOCUMENT_WRITE_CONCERN_COMMITTED)
        delete = client.calls[1][0]
        self.assertEqual(delete.index_id, "second")
        self.assertIsInstance(bson.decode(delete.ids_bson[1])["_id"], Int64)
        self.assertNotIsInstance(bson.decode(delete.ids_bson[2])["_id"], Int64)
        self.assertFalse(hasattr(upsert, "request_id"))

    def test_default_concern_is_indexed_and_visible_preserves_presence(self):
        client = RecordingClient()
        client.upsert("index", {"_id": "a"})
        client.delete("index", "a")
        client.upsert("index", {"_id": "a"}, concern="visible")
        self.assertEqual(client.calls[0][0].write_concern, api.DOCUMENT_WRITE_CONCERN_INDEXED)
        self.assertEqual(client.calls[1][0].write_concern, api.DOCUMENT_WRITE_CONCERN_INDEXED)
        visible = api.UpsertRequest.FromString(client.calls[2][0].SerializeToString())
        self.assertTrue(visible.HasField("write_concern"))
        self.assertEqual(visible.write_concern, api.DOCUMENT_WRITE_CONCERN_VISIBLE)
        self.assertFalse(api.UpsertRequest().HasField("write_concern"))

    def test_generation_and_timeout_are_forwarded_without_retries(self):
        client = RecordingClient()
        client.upsert("index", {"_id": "a"}, generation_id="generation", timeout=5)
        self.assertEqual(client.calls[0][0].generation_id, "generation")
        self.assertEqual(client.calls[0][1]["timeout"], 5)
        with self.assertRaises(ValueError):
            client.upsert("index", {"_id": "a"}, concern="anything")
        self.assertEqual(len(client.calls), 1)

    def test_delete_by_query_forwards_selector_concern_and_limits(self):
        client = RecordingClient()
        client.index("index").delete_by_query(
            {"equals": {"path": "value", "value": Int64(1)}},
            concern="committed",
            generation_id="generation",
            max_matches=7,
            dry_run=True,
            timeout=5,
        )
        request, options = client.calls[0]
        self.assertEqual(request.index_id, "index")
        self.assertEqual(request.WhichOneof("selector"), "search_query_bson")
        self.assertIsInstance(bson.decode(request.search_query_bson)["equals"]["value"], Int64)
        self.assertEqual(request.write_concern, api.DOCUMENT_WRITE_CONCERN_COMMITTED)
        self.assertEqual(request.generation_id, "generation")
        self.assertEqual(request.max_matches, 7)
        self.assertTrue(request.dry_run)
        self.assertEqual(options["timeout"], 5)
        self.assertEqual(len(client.calls), 1)

    def test_delete_by_query_vector_and_match_all_are_explicit(self):
        client = RecordingClient()
        client.index("index").delete_by_query({"tenant": "gone"}, vector=True)
        client.index("index").delete_by_query(match_all=True)
        self.assertEqual(client.calls[0][0].WhichOneof("selector"), "vector_filter_bson")
        self.assertTrue(client.calls[1][0].match_all)
        with self.assertRaises(ValueError):
            client.index("index").delete_by_query({"tenant": "gone"}, match_all=True)
        self.assertEqual(len(client.calls), 2)


if __name__ == "__main__":
    unittest.main()
