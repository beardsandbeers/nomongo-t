import type { SearchMetaRequest } from './generated/mongot/standalone/v1/SearchMetaRequest';
import type { SearchMetaResponse__Output } from './generated/mongot/standalone/v1/SearchMetaResponse';
import type { DeleteByQueryRequest } from './generated/mongot/standalone/v1/DeleteByQueryRequest';
import type { DeleteByQueryResponse__Output } from './generated/mongot/standalone/v1/DeleteByQueryResponse';
import type { UpsertRequest } from './generated/mongot/standalone/v1/UpsertRequest';
import type { UpsertResponse__Output } from './generated/mongot/standalone/v1/UpsertResponse';
import type { DeleteRequest } from './generated/mongot/standalone/v1/DeleteRequest';
import type { DeleteResponse__Output } from './generated/mongot/standalone/v1/DeleteResponse';
import * as grpc from '@grpc/grpc-js';
import * as loader from '@grpc/proto-loader';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { serialize, deserialize, Document } from 'bson';
import type { ProtoGrpcType } from './generated/standalone';
import type { CreateIndexRequest } from './generated/mongot/standalone/v1/CreateIndexRequest';
import type { IndexDescription__Output } from './generated/mongot/standalone/v1/IndexDescription';
import type { UpdateIndexRequest } from './generated/mongot/standalone/v1/UpdateIndexRequest';
import type { DropIndexRequest } from './generated/mongot/standalone/v1/DropIndexRequest';
import type { GetIndexRequest } from './generated/mongot/standalone/v1/GetIndexRequest';
import type { GetIndexMappingsRequest } from './generated/mongot/standalone/v1/GetIndexMappingsRequest';
import type { GetIndexMappingsResponse__Output } from './generated/mongot/standalone/v1/GetIndexMappingsResponse';
import type { ListIndexesRequest } from './generated/mongot/standalone/v1/ListIndexesRequest';
import type { ListIndexesResponse__Output } from './generated/mongot/standalone/v1/ListIndexesResponse';
import type { GetStatusRequest } from './generated/mongot/standalone/v1/GetStatusRequest';
import type { ServiceStatus__Output } from './generated/mongot/standalone/v1/ServiceStatus';
import type { QueryRequest } from './generated/mongot/standalone/v1/QueryRequest';
import type { QueryResponse__Output } from './generated/mongot/standalone/v1/QueryResponse';
import type { GetMoreRequest } from './generated/mongot/standalone/v1/GetMoreRequest';
import type { KillCursorRequest } from './generated/mongot/standalone/v1/KillCursorRequest';
import type { KillCursorResponse__Output } from './generated/mongot/standalone/v1/KillCursorResponse';

import type { GetIndexStatsRequest } from './generated/mongot/standalone/v1/GetIndexStatsRequest';
import type { IndexStats__Output } from './generated/mongot/standalone/v1/IndexStats';
import type { GetIndexFieldStatsRequest } from './generated/mongot/standalone/v1/GetIndexFieldStatsRequest';
import type { IndexFieldStatsResponse__Output } from './generated/mongot/standalone/v1/IndexFieldStatsResponse';

export const encodeBson = (document: Document): Buffer => Buffer.from(serialize(document));
export const decodeBson = (data: Uint8Array): Document => deserialize(data, { promoteLongs: false });
export interface ClientOptions { address?: string; caFile?: string; tokenFile?: string; timeoutMs?: number }

/** Full typed API. uint64 fields are decimal strings, preserving precision. */
export class StandaloneClient {
  readonly catalog;
  readonly documents;
  readonly queries;
  readonly operations;
  private readonly metadata = new grpc.Metadata();
  private readonly timeoutMs: number;
  constructor(options: ClientOptions = {}) {
    this.timeoutMs = options.timeoutMs ?? 30000;
    if (!(this.timeoutMs > 0) || !Number.isFinite(this.timeoutMs)) throw new Error('timeoutMs must be positive and finite');
    if (options.tokenFile && !options.caFile) throw new Error('bearer authentication requires TLS');
    if (options.tokenFile) this.metadata.set('authorization', 'Bearer ' + readFileSync(options.tokenFile, 'utf8').trim());
    const definition = loader.loadSync(join(__dirname, 'standalone.proto'), { longs: String, enums: String, defaults: true, oneofs: true });
    const api = (grpc.loadPackageDefinition(definition) as unknown as ProtoGrpcType).mongot.standalone.v1;
    const credentials = options.caFile ? grpc.credentials.createSsl(readFileSync(options.caFile)) : grpc.credentials.createInsecure();
    const address = options.address ?? '127.0.0.1:27029';
    const channelOptions = { 'grpc.max_receive_message_length': 64 * 1024 * 1024 };
    this.catalog = new api.CatalogService(address, credentials, channelOptions);
    this.documents = new api.DocumentService(address, credentials, channelOptions);
    this.operations = new api.OperationsService(address, credentials, channelOptions);
    this.queries = new api.QueryService(address, credentials, channelOptions);
  }
  close(): void { this.catalog.close(); this.documents.close(); this.queries.close(); this.operations.close(); }
  private call<T>(invoke: (metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<T>) => grpc.ClientUnaryCall, requiresContract = false, discard?: (value: T) => Promise<void>): Promise<T> {
    return new Promise((resolve, reject) => {
      const metadata = this.metadata.clone();
      let acknowledged = false;
      if (requiresContract) metadata.set('mongot-direct-read-contract', 'all-index-reads-v1');
      const call = invoke(metadata, { deadline: Date.now() + this.timeoutMs }, async (error, value) => {
        if (error) reject(error);
        else if (requiresContract && !acknowledged) {
          try {
            if (value !== undefined) await discard?.(value);
          } catch { /* Preserve the contract error if cleanup fails. */ }
          reject(new Error('Server does not support the all-index direct read contract'));
        } else if (value === undefined) reject(new Error('Missing gRPC response'));
        else resolve(value);
      });
      call.on('metadata', (headers: grpc.Metadata) => {
        acknowledged = headers.get('mongot-direct-read-contract').includes('all-index-reads-v1');
      });
    });
  }
  private discardQueryResponse(response: QueryResponse__Output): Promise<void> {
    if (!response.cursorId || response.cursorId === '0') return Promise.resolve();
    return new Promise(resolve => {
      this.queries.killCursor({ cursorId: response.cursorId }, this.metadata,
        { deadline: Date.now() + Math.min(this.timeoutMs, 5000) }, () => resolve());
    });
  }
  createIndex(request: CreateIndexRequest): Promise<IndexDescription__Output> {
    return this.call((metadata, options, callback) => this.catalog.createIndex(request, metadata, options, callback));
  }
  updateIndex(request: UpdateIndexRequest): Promise<IndexDescription__Output> {
    return this.call((metadata, options, callback) => this.catalog.updateIndex(request, metadata, options, callback));
  }
  dropIndex(request: DropIndexRequest): Promise<IndexDescription__Output> {
    return this.call((metadata, options, callback) => this.catalog.dropIndex(request, metadata, options, callback));
  }
  getIndex(request: GetIndexRequest): Promise<IndexDescription__Output> {
    return this.call((metadata, options, callback) => this.catalog.getIndex(request, metadata, options, callback));
  }
  getIndexMappings(request: GetIndexMappingsRequest): Promise<GetIndexMappingsResponse__Output> {
    return this.call((metadata, options, callback) => this.catalog.getIndexMappings(request, metadata, options, callback));
  }
  listIndexes(request: ListIndexesRequest): Promise<ListIndexesResponse__Output> {
    const ownership = request.ownership === 'INDEX_OWNERSHIP_UNSPECIFIED' ? 0 : request.ownership;
    return this.call((metadata, options, callback) => this.catalog.listIndexes(request, metadata, options, callback), !!(ownership || request.pageSize || request.pageToken));
  }
  getIndexStats(request: GetIndexStatsRequest): Promise<IndexStats__Output> {
    return this.call((metadata, options, callback) => this.catalog.getIndexStats(request, metadata, options, callback));
  }
  getIndexFieldStats(request: GetIndexFieldStatsRequest): Promise<IndexFieldStatsResponse__Output> {
    return this.call((metadata, options, callback) => this.catalog.getIndexFieldStats(request, metadata, options, callback));
  }
  getStatus(request: GetStatusRequest): Promise<ServiceStatus__Output> {
    return this.call((metadata, options, callback) => this.operations.getStatus(request, metadata, options, callback));
  }
  search(request: QueryRequest): Promise<QueryResponse__Output> {
    return this.call((metadata, options, callback) => this.queries.search(request, metadata, options, callback), !!request.joinIndexes?.length, response => this.discardQueryResponse(response));
  }
  searchMeta(request: SearchMetaRequest): Promise<SearchMetaResponse__Output> {
    return this.call((metadata, options, callback) => this.queries.searchMeta(request, metadata, options, callback), !!(request.targetIndexId || request.joinIndexes?.length));
  }
  vectorSearch(request: QueryRequest): Promise<QueryResponse__Output> {
    return this.call((metadata, options, callback) => this.queries.vectorSearch(request, metadata, options, callback), !!request.joinIndexes?.length, response => this.discardQueryResponse(response));
  }
  getMore(request: GetMoreRequest): Promise<QueryResponse__Output> {
    return this.call((metadata, options, callback) => this.queries.getMore(request, metadata, options, callback));
  }
  killCursor(request: KillCursorRequest): Promise<KillCursorResponse__Output> {
    return this.call((metadata, options, callback) => this.queries.killCursor(request, metadata, options, callback));
  }
  upsert(request: UpsertRequest): Promise<UpsertResponse__Output> {
    return this.call((metadata, options, callback) => this.documents.upsert(request, metadata, options, callback));
  }
  deleteByQuery(request: DeleteByQueryRequest): Promise<DeleteByQueryResponse__Output> {
    return this.call((metadata, options, callback) => this.documents.deleteByQuery(request, metadata, options, callback));
  }
  delete(request: DeleteRequest): Promise<DeleteResponse__Output> {
    return this.call((metadata, options, callback) => this.documents.delete(request, metadata, options, callback));
  }
}
