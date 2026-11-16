require('./formatting')
  .format(false)
  .catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
