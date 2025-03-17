import {type SingleConnectionRuntime} from '@malloydata/malloy';
import {RuntimeList} from '../../runtimes';
import {describeIfDatabaseAvailable} from '../../util';
import {type SqliteConnection} from '@malloydata/db-sqlite';
import '../../util/db-jest-matchers';

const [describe] = describeIfDatabaseAvailable(['sqlite']);

describe('Sqlite tests', () => {
  const runtimeList = new RuntimeList(['sqlite']);
  const runtime = runtimeList.runtimeMap.get('sqlite');
  const typedRuntime = runtime as SingleConnectionRuntime<SqliteConnection>;

  if (runtime === undefined) {
    throw new Error("Couldn't build runtime");
  }

  beforeAll(async () => {
    await runtime.connection.runSQL('SELECT 1');
  });

  afterAll(async () => {
    await runtimeList.closeAll();
  });

  it('runs an sql query', async () => {
    await expect(
      'run: sqlite.sql("SELECT 1 as n") -> { select: n }'
    ).malloyResultMatches(runtime, {n: 1});
  });

  it('has an attached database for tests', async () => {
    const databases = await typedRuntime.connection.getDatabases();
    expect(databases).toContain('main');
    expect(databases).toContain('malloytest');
  });

  it('has airports', async () => {
    const res = await typedRuntime.connection.runSQL(
      'SELECT * FROM malloytest.airports LIMIT 10'
    );

    expect(res.rows.length).toBe(10);
  });
});
