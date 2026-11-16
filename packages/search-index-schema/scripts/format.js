require('./formatting')
  .format(true)
  .catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
