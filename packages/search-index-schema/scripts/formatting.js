const fs = require('node:fs');
const path = require('node:path');
const { globSync } = require('glob');
const prettier = require('prettier');

async function format(write) {
  const root = path.resolve(__dirname, '..');
  const files = globSync(
    ['schema/**/*.json', 'scripts/**/*.js', 'test/**/*.json', '*.{json,js,ts,md}'],
    { cwd: root, nodir: true }
  ).sort();
  let failures = 0;
  for (const relative of files) {
    const file = path.join(root, relative);
    const info = await prettier.getFileInfo(file, {
      ignorePath: path.join(root, '.prettierignore'),
    });
    if (info.ignored) continue;
    const options = { ...(await prettier.resolveConfig(file)), filepath: file };
    const source = fs.readFileSync(file, 'utf8');
    if (write) fs.writeFileSync(file, await prettier.format(source, options));
    else if (!(await prettier.check(source, options))) {
      console.error(relative);
      failures++;
    }
  }
  if (failures) throw new Error(`${failures} files need formatting`);
}

module.exports = { format };
