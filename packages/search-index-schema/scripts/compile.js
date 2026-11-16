const { compileSources, variantFiles } = require('./schema_tools');

compileSources();
console.log(`Compiled all source schemas and ${variantFiles.length} variants.`);
