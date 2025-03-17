// duckdb node bindings do not come with Typescript types, require is required
// https://github.com/duckdb/duckdb/tree/master/tools/nodejs
// eslint-disable-next-line @typescript-eslint/no-var-requires
import {string} from 'yargs';
import {DuckDBConnection} from '../packages/malloy-db-duckdb/dist';
import {SqliteConnection} from '../packages/malloy-db-sqlite/dist';

import fs from 'fs';

// Process here is to leverage ATTACH and parquet_scan to load data from parquet files into the database
// via duckdb. However duckdb does not support DATE / TIMESTAMP types for attached sql lite tables (these
// get transparently converted to TEXT). This is a limitation of duckdb, not malloy.
//
// To work around this, we do a second round of conversion with the sqlite connection, which will
// then remap columns to appropriate types, and drop the old columns.

// Original parquet files are located in test/data/duckdb
const sourceCwd = './test/data/duckdb/';

// Destination database
const cwd = './test/data/sqlite/';
const databasePath = `${cwd}sqlite_test.db`;
if (fs.existsSync(databasePath)) {
  console.log(`Database at ${databasePath} already exists, removing`);
  fs.rmSync(databasePath);
}

const duckdb = new DuckDBConnection({
  name: 'duckdb',
});
const sqlite = new SqliteConnection('sqlite', {dbPath: databasePath});

const runDuckDb = (sql: string) => {
  return duckdb.runRawSQL(sql);
};

const runSqlite = (sql: string) => {
  console.log(`   -- ${sql}`);
  return sqlite.executeSQL(sql);
};

type SourceUpdate = {
  column: string;
  dest_type: string;
  expr: string;
};

const SOURCES: Record<string, SourceUpdate[]> = {
  aircraft: [
    {
      column: 'last_action_date',
      dest_type: 'DATE',
      expr: 'DATE(last_action_date)',
    },
    {
      column: 'cert_issue_date',
      dest_type: 'DATE',
      expr: 'DATE(cert_issue_date)',
    },
    {
      column: 'air_worth_date',
      dest_type: 'DATE',
      expr: 'DATE(air_worth_date)',
    },
  ],
  flights: [
    {
      column: 'dep_time',
      dest_type: 'DATETIME',
      expr: 'DATETIME(dep_time)',
    },
    {
      column: 'arr_time',
      dest_type: 'DATETIME',
      expr: 'DATETIME(arr_time)',
    },
  ],
  alltypes: [
    {
      column: 't_numeric',
      dest_type: 'NUMERIC',
      expr: 'CAST(t_numeric AS NUMERIC)',
    },
    {
      column: 't_bignumeric',
      dest_type: 'DECIMAL(38,9)',
      expr: 'CAST(t_bignumeric AS DECIMAL(38,9))',
    },
    {
      column: 't_bool_true',
      dest_type: 'BOOLEAN',
      expr: 'CAST(t_bool_true AS BOOLEAN)',
    },
    {
      column: 't_bool_false',
      dest_type: 'BOOLEAN',
      expr: 'CAST(t_bool_false AS BOOLEAN)',
    },
    {
      column: 't_bool_null',
      dest_type: 'BOOLEAN',
      expr: 'CAST(t_bool_null AS BOOLEAN)',
    },
  ],
  alltypes2: [], // TODO:
  ga_sample: [], // TODO: Structs :(
  // No updates needed
  carriers: [],
  bq_medicare_test: [],
  numbers: [],
  airports: [],
  aircraft_models: [],
  state_facts: [],
  words: [],
  words_bigger: [],
};

console.log(`Creating database at ${databasePath}`);

(async () => {
  try {
    console.log('Import parquet files to sqlite via duckdb');

    await runDuckDb(`ATTACH '${databasePath}' AS malloytest (TYPE SQLITE)`);

    for (const source of Object.keys(SOURCES)) {
      console.log(`Importing ${source} to sqlite via duckdb`);
      await runDuckDb(
        `CREATE TABLE malloytest.${source} AS SELECT * FROM parquet_scan('${sourceCwd}${source}.parquet')`
      );
    }

    console.log('Finished populating database with data from parqeut files');

    await duckdb.close();

    for (const [source, updates] of Object.entries(SOURCES)) {
      // Make sure we have atleast one row
      const select_count = await sqlite.runSQL(
        `SELECT COUNT(*) as count FROM ${source}`
      );

      const observed_count = select_count.rows?.[0]?.['count'];
      if (!observed_count) {
        throw new Error(`Table ${source} is empty`);
      } else {
        console.log(`Table ${source} has ${observed_count} rows`);
      }

      // Execute column updates
      for (const update of updates) {
        const {column, dest_type, expr} = update;
        const temp_column = `${column}_temp`;
        const drop_column = `${column}_drop`;
        console.log(`Updating column ${column} in ${source} to ${dest_type}`);

        // Add a new column with the new type, fill it with the converted values
        // Rename the old column to a temp name, rename the new column to the old name
        // Drop the old column

        await runSqlite(
          `ALTER TABLE ${source} ADD COLUMN ${temp_column} ${dest_type}`
        );
        await runSqlite(`UPDATE ${source} SET ${temp_column} = ${expr}`);
        await runSqlite(
          `ALTER TABLE ${source} RENAME COLUMN ${column} TO ${drop_column}`
        );
        await runSqlite(
          `ALTER TABLE ${source} RENAME COLUMN ${temp_column} TO ${column}`
        );
        await runSqlite(`ALTER TABLE ${source} DROP COLUMN ${drop_column}`);
      }
    }

    await sqlite.close();
  } catch (e) {
    console.log(e);
  }
})();
