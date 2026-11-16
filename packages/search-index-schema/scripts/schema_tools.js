const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const Ajv = require('ajv');
const { globSync } = require('glob');

const packageRoot = path.resolve(__dirname, '..');
const schemaRoot = path.join(packageRoot, 'schema');
const variants = require('../variants.json');
const variantFiles = [
  ...new Set(Object.values(variants).flatMap((entries) => entries.map((v) => v.schema))),
];

function createAjv() {
  // Editor annotations such as markdownDescription are intentional extensions to draft-07.
  return new Ajv({ strict: false, allErrors: true });
}

function compileSources() {
  const ajv = createAjv();
  const files = globSync('**/*.json', { cwd: schemaRoot }).sort();
  assert(files.length > 0, 'No source schemas found');
  for (const file of files) {
    ajv.addSchema(JSON.parse(fs.readFileSync(path.join(schemaRoot, file), 'utf8')), file);
  }
  for (const file of files) assert(ajv.getSchema(file), `Could not compile ${file}`);
  for (const file of variantFiles) assert(ajv.getSchema(file), `Missing variant ${file}`);
  return ajv;
}

async function writeBundles(outputDirectory) {
  const { $RefParser } = await import('@apidevtools/json-schema-ref-parser');
  for (const file of variantFiles) {
    const bundled = await $RefParser.bundle(path.join(schemaRoot, file), {
      resolve: { http: false },
    });
    const rootId = bundled.$id;
    // Embedded $id values change the base of JSON pointers. Give the bundle one identity and
    // keep its references local, including the recursive document-mapping references.
    const schema = JSON.parse(
      JSON.stringify(bundled, (key, value) => {
        if (key === '$id' && typeof value === 'string') return undefined;
        if (key === '$ref' && typeof value === 'string') {
          const local = value.startsWith(`${rootId}#`) ? value.slice(rootId.length) : value;
          assert(local.startsWith('#'), `Unbundled reference in ${file}: ${value}`);
          return local;
        }
        return value;
      })
    );
    schema.$id = rootId;
    createAjv().compile(schema);
    const destination = path.join(outputDirectory, file);
    fs.mkdirSync(path.dirname(destination), { recursive: true });
    fs.writeFileSync(destination, `${JSON.stringify(schema, null, 2)}\n`);
  }
  const imports = variantFiles.map(
    (file) => `  ${JSON.stringify(file)}: require(${JSON.stringify(`./${file}`)}),`
  );
  fs.writeFileSync(
    path.join(outputDirectory, 'schemas.generated.js'),
    `module.exports = {\n${imports.join('\n')}\n};\n`
  );
}

function validateFixtures(resolveSchema) {
  const files = globSync('test/**/*.json', { cwd: packageRoot }).sort();
  assert(files.length > 0, 'No schema validation fixtures found');
  let count = 0;
  for (const file of files) {
    const fixtures = JSON.parse(fs.readFileSync(path.join(packageRoot, file), 'utf8'));
    for (const fixture of fixtures) {
      const validate = resolveSchema(fixture.indexType, fixture.flags || []);
      const valid = validate(fixture.definition);
      assert.equal(valid, fixture.valid, `${fixture.name}: ${JSON.stringify(validate.errors)}`);
      count++;
    }
  }
  assert(count > 0, 'No schema validation cases found');
  return count;
}

function selectVariant(indexType, enabledFlags) {
  const enabled = new Set(enabledFlags);
  return variants[indexType].find((candidate) => candidate.flags.every((flag) => enabled.has(flag)))
    .schema;
}

module.exports = {
  packageRoot,
  variants,
  variantFiles,
  createAjv,
  compileSources,
  writeBundles,
  validateFixtures,
  selectVariant,
};
