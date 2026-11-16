const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {
  packageRoot,
  variants,
  createAjv,
  writeBundles,
  validateFixtures,
} = require('./schema_tools');

async function main() {
  const temporary = fs.mkdtempSync(
    path.join(process.env.TEST_TMPDIR || os.tmpdir(), 'schema-bundle-')
  );
  try {
    await writeBundles(path.join(temporary, 'output'));
    for (const file of ['index.js', 'variants.json'])
      fs.copyFileSync(path.join(packageRoot, file), path.join(temporary, file));
    const { getSchema } = require(path.join(temporary, 'index.js'));
    for (const [indexType, entries] of Object.entries(variants)) {
      for (const variant of entries) {
        const schema = getSchema(indexType, variant.flags);
        assert.equal(schema.$id, variant.schema);
        createAjv().compile(schema);
        schema.title = 'mutated';
        assert.notEqual(getSchema(indexType, variant.flags).title, 'mutated');
      }
      assert.deepEqual(getSchema(indexType, ['unknownFlag']), getSchema(indexType));
    }
    assert.throws(() => getSchema('notAnIndex'), /Unknown index type/);
    assert.throws(() => getSchema('__proto__'), /Unknown index type/);
    const count = validateFixtures((indexType, flags) =>
      createAjv().compile(getSchema(indexType, flags))
    );
    console.log(
      `Validated package loading, variant selection, independent copies and ${count} bundled-schema cases.`
    );
  } finally {
    fs.rmSync(temporary, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
