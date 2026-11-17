const assert = require('node:assert/strict');
const path = require('node:path');
const { test } = require('node:test');
const grpc = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const { StandaloneClient } = require('../dist/client');

const definition = loader.loadSync(path.join(__dirname, '../dist/standalone.proto'), {
  longs: String, enums: String, defaults: true, oneofs: true,
});
const api = grpc.loadPackageDefinition(definition).mongot.standalone.v1;
const contractKey = 'mongot-direct-read-contract';
const contract = 'all-index-reads-v1';

async function fixture(t, acknowledged = false) {
  const seen = [];
  const server = new grpc.Server();
  server.addService(api.CatalogService.service, {
    listIndexes(call, callback) {
      seen.push({ request: call.request, contract: call.metadata.get(contractKey) });
      if (acknowledged) {
        const headers = new grpc.Metadata();
        headers.set(contractKey, contract);
        call.sendMetadata(headers);
      }
      callback(null, { indexes: [] });
    },
  });
  t.after(() => new Promise(resolve => server.tryShutdown(resolve)));
  const port = await new Promise((resolve, reject) => {
    server.bindAsync('127.0.0.1:0', grpc.ServerCredentials.createInsecure(),
      (error, port) => error ? reject(error) : resolve(port));
  });
  const client = new StandaloneClient({ address: `127.0.0.1:${port}`, timeoutMs: 5000 });
  t.after(() => client.close());
  return { client, seen };
}

for (const [name, request] of [
  ['omitted', {}],
  ['numeric zero', { ownership: 0 }],
  ['string default', { ownership: 'INDEX_OWNERSHIP_UNSPECIFIED' }],
]) {
  test(`listIndexes accepts ${name} ownership without a contract acknowledgement`, async t => {
    const { client, seen } = await fixture(t);

    const result = await client.listIndexes(request);

    assert.deepEqual(result.indexes, []);
    assert.equal(seen[0].request.ownership, 'INDEX_OWNERSHIP_UNSPECIFIED');
    assert.deepEqual(seen[0].contract, []);
  });
}

for (const [name, request] of [
  ['numeric owner', { ownership: 2 }],
  ['string owner', { ownership: 'INDEX_OWNERSHIP_REPLICATED' }],
  ['page size with string default', { ownership: 'INDEX_OWNERSHIP_UNSPECIFIED', pageSize: 1 }],
  ['page token with string default', { ownership: 'INDEX_OWNERSHIP_UNSPECIFIED', pageToken: 'next' }],
]) {
  test(`listIndexes rejects an unacknowledged ${name}`, async t => {
    const { client, seen } = await fixture(t);

    await assert.rejects(client.listIndexes(request), /does not support the all-index direct read contract/);

    assert.deepEqual(seen[0].contract, [contract]);
  });
}

test('listIndexes accepts an acknowledged filter and pagination', async t => {
  const { client, seen } = await fixture(t, true);

  const result = await client.listIndexes({ ownership: 'INDEX_OWNERSHIP_REPLICATED', pageSize: 1 });

  assert.deepEqual(result.indexes, []);
  assert.deepEqual(seen[0].contract, [contract]);
  assert.equal(seen[0].request.ownership, 'INDEX_OWNERSHIP_REPLICATED');
  assert.equal(seen[0].request.pageSize, 1);
});
