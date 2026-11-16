const path = require('node:path');
const { packageRoot, writeBundles } = require('./schema_tools');

writeBundles(process.env.BUNDLE_OUTPUT_DIR || path.join(packageRoot, 'output')).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
