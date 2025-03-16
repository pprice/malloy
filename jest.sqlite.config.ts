module.exports = {
  ...require('./jest.config.ts'),
  roots: [
    '<rootDir>/packages/malloy-db-sqlite/',
    '<rootDir>/test/src/databases/all/',
    '<rootDir>/test/src/databases/sqlite/',
  ],
};
