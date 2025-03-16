import {RuntimeList} from '../../runtimes';
import {describeIfDatabaseAvailable} from '../../util';

const [describe] = describeIfDatabaseAvailable(['postgres']);

describe('Sqlite tests', () => {
  const runtimeList = new RuntimeList(['sqlite']);
  const runtime = runtimeList.runtimeMap.get('sqlite');

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
});
