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
    registerUserDefinedFunctions(this.db);
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

  runSQL(
    sql: string,
    _options?: RunSQLOptions | undefined
  ): Promise<MalloyQueryData> {
    return this.runRawSQL(sql, _options).catch(e => {
      this.verboseLog(() => ['Error running SQL', sql, 'Error', e]);
      this.dumpDatabaseState();
      throw e;
    });
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
        .prepare<unknown[], {name: string}>(
          `SELECT name FROM ${db.name}.sqlite_master WHERE type='table'`
        )
        .all()
        .map(row => row['name']);
    });

    // Verbose log the db state
    this.verboseLog(() => [
      'Databases:\n',
      ...databases.map(db => db.name),
      'Tables:\n',
      tables,
    ]);
  }

  private async runRawSQL(
    sql: string,
    _options?: RunSQLOptions | undefined
  ): Promise<MalloyQueryData> {
    // First generic param is args, second is return type
    // First generic param is args, second is return type
    const statement = this.db.prepare<unknown[], QueryDataRow>(sql);
    const rows = statement.all();

    this.verboseLog(() => sql);

    const result: MalloyQueryData = {
      rows: rows,
      totalRows: rows.length,
    };

    return result;
  }

  public executeSQL(sql: string): void {
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
