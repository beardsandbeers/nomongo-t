"""Typed requests/responses, BSON helpers, per-call deadlines and TLS/bearer auth."""

import math
from collections.abc import Mapping
from pathlib import Path

import bson
import grpc

from . import standalone_pb2 as api
from . import standalone_pb2_grpc as rpc
from .documents import DocumentCommandsMixin


def encode_bson(document: Mapping) -> bytes:
    return bson.encode(document)


def decode_bson(data: bytes) -> dict:
    return bson.decode(data)


class Client(DocumentCommandsMixin):
    def __init__(self, address="127.0.0.1:27029", *, ca_file=None, token_file=None, timeout=30):
        if timeout <= 0 or not math.isfinite(timeout):
            raise ValueError("timeout must be positive and finite")
        if token_file and not ca_file:
            raise ValueError("bearer authentication requires TLS")
        self.timeout = timeout
        self.metadata = (
            (("authorization", "Bearer " + Path(token_file).read_text().strip()),)
            if token_file
            else ()
        )
        options = (("grpc.max_receive_message_length", 64 * 1024 * 1024),)
        self.channel = (
            grpc.secure_channel(
                address,
                grpc.ssl_channel_credentials(Path(ca_file).read_bytes()),
                options,
            )
            if ca_file
            else grpc.insecure_channel(address, options)
        )
        self.catalog = rpc.CatalogServiceStub(self.channel)
        self.documents = rpc.DocumentServiceStub(self.channel)
        self.operations = rpc.OperationsServiceStub(self.channel)
        self.queries = rpc.QueryServiceStub(self.channel)

    def close(self):
        self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def _read(self, method, request, timeout, requires_contract=False):
        if not requires_contract:
            return method(
                request,
                timeout=self.timeout if timeout is None else timeout,
                metadata=self.metadata,
            )
        response, call = method.with_call(
            request,
            timeout=self.timeout if timeout is None else timeout,
            metadata=self.metadata + (("mongot-direct-read-contract", "all-index-reads-v1"),),
        )
        if dict(call.initial_metadata()).get("mongot-direct-read-contract") != "all-index-reads-v1":
            if isinstance(response, api.QueryResponse) and response.cursor_id:
                try:
                    self.queries.KillCursor(
                        api.KillCursorRequest(cursor_id=response.cursor_id),
                        timeout=min(self.timeout if timeout is None else timeout, 5),
                        metadata=self.metadata,
                    )
                except grpc.RpcError:
                    pass  # Preserve the contract error even if the server cannot release its cursor.
            raise RuntimeError("Server does not support the all-index direct read contract")
        return response

    def create_index(
        self, request: api.CreateIndexRequest, *, timeout=None
    ) -> api.IndexDescription:
        return self.catalog.CreateIndex(
            request,
            timeout=self.timeout if timeout is None else timeout,
            metadata=self.metadata,
        )

    def update_index(
        self, request: api.UpdateIndexRequest, *, timeout=None
    ) -> api.IndexDescription:
        return self.catalog.UpdateIndex(
            request,
            timeout=self.timeout if timeout is None else timeout,
            metadata=self.metadata,
        )

    def drop_index(self, request: api.DropIndexRequest, *, timeout=None) -> api.IndexDescription:
        return self.catalog.DropIndex(
            request,
            timeout=self.timeout if timeout is None else timeout,
            metadata=self.metadata,
        )

    def get_index(self, request: api.GetIndexRequest, *, timeout=None) -> api.IndexDescription:
        return self.catalog.GetIndex(
            request,
            timeout=self.timeout if timeout is None else timeout,
            metadata=self.metadata,
        )

    def list_indexes(
        self, request: api.ListIndexesRequest, *, timeout=None
    ) -> api.ListIndexesResponse:
        return self._read(
            self.catalog.ListIndexes,
            request,
            timeout,
            bool(request.ownership or request.page_size or request.page_token),
        )

    def get_index_mappings(
        self, request: api.GetIndexMappingsRequest, *, timeout=None
    ) -> api.GetIndexMappingsResponse:
        return self.catalog.GetIndexMappings(
            request,
            timeout=self.timeout if timeout is None else timeout,
            metadata=self.metadata,
        )

    def get_index_stats(self, request: api.GetIndexStatsRequest, *, timeout=None) -> api.IndexStats:
        return self.catalog.GetIndexStats(
            request,
            timeout=self.timeout if timeout is None else timeout,
            metadata=self.metadata,
        )

    def get_index_field_stats(
        self, request: api.GetIndexFieldStatsRequest, *, timeout=None
    ) -> api.IndexFieldStatsResponse:
        return self.catalog.GetIndexFieldStats(
            request,
            timeout=self.timeout if timeout is None else timeout,
            metadata=self.metadata,
        )

    def get_status(self, request: api.GetStatusRequest, *, timeout=None) -> api.ServiceStatus:
        return self.operations.GetStatus(
            request,
            timeout=self.timeout if timeout is None else timeout,
            metadata=self.metadata,
        )

    def search(self, request: api.QueryRequest, *, timeout=None) -> api.QueryResponse:
        return self._read(self.queries.Search, request, timeout, bool(request.join_indexes))

    def search_meta(
        self, request: api.SearchMetaRequest, *, timeout=None
    ) -> api.SearchMetaResponse:
        return self._read(
            self.queries.SearchMeta,
            request,
            timeout,
            bool(request.target_index_id or request.join_indexes),
        )

    def vector_search(self, request: api.QueryRequest, *, timeout=None) -> api.QueryResponse:
        return self._read(self.queries.VectorSearch, request, timeout, bool(request.join_indexes))

    def get_more(self, request: api.GetMoreRequest, *, timeout=None) -> api.QueryResponse:
        return self.queries.GetMore(
            request,
            timeout=self.timeout if timeout is None else timeout,
            metadata=self.metadata,
        )

    def kill_cursor(
        self, request: api.KillCursorRequest, *, timeout=None
    ) -> api.KillCursorResponse:
        return self.queries.KillCursor(
            request,
            timeout=self.timeout if timeout is None else timeout,
            metadata=self.metadata,
        )
