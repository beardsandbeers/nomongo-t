import unittest
from types import SimpleNamespace

from mongot_standalone import Client, api


class Method:
    def __init__(self, acknowledged):
        self.acknowledged = acknowledged
        self.calls = []

    def __call__(self, request, **options):
        self.calls.append((request, options))
        return "legacy"

    def with_call(self, request, **options):
        self.calls.append((request, options))
        return "result", SimpleNamespace(initial_metadata=lambda: self.acknowledged)


class ReadContractTest(unittest.TestCase):
    def client(self, method):
        client = Client.__new__(Client)
        client.timeout = 10
        client.metadata = ()
        client.queries = SimpleNamespace(SearchMeta=method, Search=method)
        client.catalog = SimpleNamespace(ListIndexes=method)
        return client

    def test_explicit_target_rejects_old_server_without_legacy_fallback(self):
        method = Method(())
        request = api.SearchMetaRequest(target_index_id="0123456789abcdef01234567")
        with self.assertRaisesRegex(RuntimeError, "does not support"):
            self.client(method).search_meta(request)
        self.assertEqual(len(method.calls), 1)

    def test_explicit_target_requires_matching_contract(self):
        method = Method((("mongot-direct-read-contract", "all-index-reads-v1"),))
        request = api.SearchMetaRequest(target_index_id="0123456789abcdef01234567")
        self.assertEqual(self.client(method).search_meta(request), "result")
        self.assertIn(
            ("mongot-direct-read-contract", "all-index-reads-v1"), method.calls[0][1]["metadata"]
        )

    def test_pagination_rejects_ignored_fields_on_old_server(self):
        method = Method(())
        with self.assertRaisesRegex(RuntimeError, "does not support"):
            self.client(method).list_indexes(api.ListIndexesRequest(page_size=1))

    def test_legacy_query_remains_usable_without_contract(self):
        method = Method(())
        self.assertEqual(self.client(method).search_meta(api.SearchMetaRequest()), "legacy")
        self.assertEqual(method.calls[0][1]["metadata"], ())

    def test_join_binding_rejects_ignored_fields_on_old_server(self):
        method = Method(())
        request = api.QueryRequest(
            join_indexes=[
                api.JoinIndexBinding(alias="inventory", index_id="0123456789abcdef01234567")
            ]
        )
        with self.assertRaisesRegex(RuntimeError, "does not support"):
            self.client(method).search(request)
