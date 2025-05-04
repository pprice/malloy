import {BaseConnection} from '@malloydata/malloy/connection';
import {
  type RunSQLOptions,
  type MalloyQueryData,
  type TableSourceDef,
  type SQLSourceRequest,
  type SQLSourceDef,
  type QueryDataRow,
  sqlKey,
  type FieldDef,
  SqliteDialect,
  QueryValue,
} from '@malloydata/malloy';

import SqliteDatabase, {type ColumnDefinition} from 'better-sqlite3';

import semvar from 'semver';
import {registerUserDefinedFunctions} from './udf_functions';

type PragmaTableInfo = {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
};

type RawMalloyQueryData = MalloyQueryData & {
  cols: ColumnDefinition[];
};

interface SqliteConnectionOptions {
  dbPath?: string;
  attachPaths?: Record<string, string>; // namespace -> path;
  readonly?: boolean;
  fileMustExist?: boolean;
}

const SQLITE_MIN_VERSION = new semvar.SemVer('3.38.0');

// If true, we'll log a bunch of debug information
const VERBOSE = true;

export class SqliteConnection extends BaseConnection {
  public name: string;
  private db: SqliteDatabase.Database;
  private readonly dialet = new SqliteDialect();

  constructor(name: string, options: SqliteConnectionOptions) {
    super();
    this.name = name;

    this.db = new SqliteDatabase(options.dbPath || ':memory:', {
      // These are required to be boolean
      readonly: !!options.readonly || false,
      fileMustExist: !!options.fileMustExist || false,
    });

    // If there are any attach paths, we need to attach them
    if (options.attachPaths) {
      for (const [namespace, path] of Object.entries(options.attachPaths)) {
        this.verboseLog(() => [
          `Attaching database ${namespace} at ${path}`,
          `ATTACH DATABASE '${path}' AS ${namespace}`,
        ]);
        this.db.prepare(`ATTACH DATABASE '${path}' AS ${namespace}`).run();
      }
    }

    this.validateMinimumVersion();

    // Register user defined functions; used to squash quirks in the sqlite dialect
    // and to add some extra functionality
    this.verboseLog(() => 'Registering user defined functions...');
    const registered = registerUserDefinedFunctions(this.db);
    this.verboseLog(() => ['Registered user defined functions', registered]);
  }

  public async getDatabases(): Promise<string[]> {
    const res = this.db
      .prepare<unknown[], {name: string}>('PRAGMA database_list')
      .all();
    return res.map(row => row.name);
  }

  public async test(): Promise<void> {
    const res = this.db.prepare('SELECT 1').all();

    if (res.length !== 1) {
      throw new Error('Failed to run test query');
    }

    return undefined;
  }

  async runSQL(
    sql: string,
    _options: RunSQLOptions = {}
  ): Promise<MalloyQueryData> {
    const res = await this.runRawSQL(sql, _options).catch(e => {
      this.verboseLog(() => ['Error running SQL', sql, 'Error', e]);
      this.dumpDatabaseState();
      throw e;
    });

    // Scan the result for any json objects :-(
    for (const row of res.rows) {
      for (const col of Object.keys(row)) {
        if (typeof row[col] === 'string') {
          row[col] = this.destringify(row[col]) as QueryValue;
        }
      }
    }

    return res;
  }

  private isLikelyJson(value: string): boolean {
    return (
      !!value &&
      ((value.startsWith('{') && value.endsWith('}')) ||
        (value.startsWith('[') && value.endsWith(']')))
    );
  }

  private destringify<T>(value: T): T {
    // If the value is a string, try to parse it as JSON
    // if the value is an object, we need to check each property
    // and attempt to destringify it
    // This makes me sad
    if (typeof value === 'string' && this.isLikelyJson(value)) {
      try {
        return this.destringify(JSON.parse(value));
      } catch (e) {
        return value;
      }
    } else if (typeof value === 'object' && value) {
      // Check if the value is an array
      if (Array.isArray(value)) {
        return value.map(v => this.destringify(v)) as T;
      }

      // Check if actual object
      for (const key of Object.keys(value)) {
        value[key] = this.destringify(value[key]);
      }
    }

    return value;
  }

  private dumpDatabaseState(): void {
    if (!VERBOSE) {
      return;
    }

    const databases = this.db
      .prepare<unknown[], {name: string}>('PRAGMA database_list')
      .all();

    const tables = databases.map(db => {
      return this.db
        .prepare<unknown[], {fb: string[]; db: string; name: string}>(
          `SELECT '${db.name}' as db, name FROM ${db.name}.sqlite_master WHERE type='table'`
        )
        .all()
        .map(row => ({
          fn: row.db + '.' + row.name,
          db: row.db,
          name: row.name,
        }));
    });

    // Verbose log the db state
    this.verboseLog(() => [
      'Databases:\n',
      ...databases.map(db => db.name),
      'Tables:\n',
      tables.map(db => db.map(t => t.fn)),
    ]);
  }

  private async runRawSQL(
    sql: string,
    _options: RunSQLOptions = {}
  ): Promise<RawMalloyQueryData> {
    // First generic param is args, second is return type

    // This will throw if the sql is invalid
    const statement = this.db.prepare<unknown[], QueryDataRow>(sql);

    const cols = statement.columns();
    const rows = statement.all();

    this.verboseLog(() => [sql, cols, rows]);

    const result: RawMalloyQueryData = {
      rows: rows,
      cols,
      totalRows: rows.length,
    };

    return result;
  }

  public executeSQL(sql: string): void {
    // This is for testing purposes only
    const statement = this.db.prepare(sql);
    statement.run();
  }

  get dialectName(): string {
    return 'sqlite';
  }

  async fetchTableSchema(
    tableName: string,
    tablePath: string
  ): Promise<TableSourceDef | string> {
    // There are two approaches here, we could do a "select * from table limit 1"
    // and then introspect the columns, or we could use the PRAGMA table_info
    // command.

    // In SQLLite it's ${schema}.${table} for the table name, so we need to
    // split the tablePath into schema and table

    // TODO: Check if we need to actually escape the table name
    const {db, table} = this.dialet.splitPath(tablePath);

    // Probably need a nicer way to do this...
    const command = db
      ? `PRAGMA ${db}.table_info(${table})`
      : `PRAGMA table_info(${table})`;
    const schema = this.db.prepare<unknown[], PragmaTableInfo>(command).all();

    const structDef: TableSourceDef = {
      type: 'table',
      name: tableName,
      tablePath,
      dialect: this.dialectName,
      connection: this.name,
      fields: schema.map(c => {
        return {
          ...this.dialet.sqlTypeToMalloyType(c.type),
          name: c.name,
        };
      }),
    };

    return structDef;
  }

  async fetchSelectSchema(
    sqlSource: SQLSourceRequest
  ): Promise<SQLSourceDef | string> {
    // Prepared queries are not executed until we start iterating over them
    // so we can prepare the query and observe the output schema without
    // actually running the query.

    const statement = this.db.prepare(sqlSource.selectStr);
    const columns = statement.columns();

    const structDef: SQLSourceDef = {
      type: 'sql_select',
      ...sqlSource,
      dialect: this.dialectName,
      name: sqlKey(sqlSource.connection, sqlSource.selectStr),
      fields: columns.map(c => this.sqlLiteColumnToField(c)),
    };
    return structDef;
  }

  async close(): Promise<void> {
    this.db.close();
    return undefined;
  }

  private sqlLiteColumnToField(value: ColumnDefinition): FieldDef {
    return {
      ...this.dialet.sqlTypeToMalloyType(value.type || ''),
      name: value.name,
    };
  }

  private validateMinimumVersion(): void {
    const version_result = this.db
      .prepare<unknown[], {version: string}>(
        'SELECT sqlite_version() as version'
      )
      .get();

    const version_string = version_result?.version;

    if (!version_string) {
      throw new Error(
        `Failed to get sqlite version; got ${JSON.stringify(version_result)}`
      );
    }

    if (SQLITE_MIN_VERSION.compare(version_string) === 1) {
      throw new Error(
        `Database is not at least version ${SQLITE_MIN_VERSION} but got ${version_result}`
      );
    }
  }

  private verboseLog(fn: () => unknown[] | unknown): void {
    if (VERBOSE) {
      const res = fn();
      if (Array.isArray(res)) {
        console.debug(...res);
        return;
      }

      console.debug(res);
    }
  }
}
