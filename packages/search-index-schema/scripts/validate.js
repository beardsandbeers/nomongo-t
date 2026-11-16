const { compileSources, validateFixtures, selectVariant } = require('./schema_tools');

const ajv = compileSources();
const count = validateFixtures((indexType, flags) =>
  ajv.getSchema(selectVariant(indexType, flags))
);
console.log(`Validated ${count} source-schema cases.`);
