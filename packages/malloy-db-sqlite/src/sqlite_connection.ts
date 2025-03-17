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
  StandardSQLDialect,
} from '@malloydata/malloy';

import SqliteDatabase, {type ColumnDefinition} from 'better-sqlite3';

import semvar from 'semver';

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

export class SqliteConnection extends BaseConnection {
  public name: string;
  private db: SqliteDatabase.Database;
  private readonly dialet = new StandardSQLDialect();

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
        this.db.prepare(`ATTACH DATABASE '${path}' AS ${namespace}`).run();
      }
    }

    this.validateMinimumVersion();
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
    // First generic param is args, second is return type
    const statement = this.db.prepare<unknown[], QueryDataRow>(sql);
    const rows = statement.all();

    const result: MalloyQueryData = {
      rows: rows,
      totalRows: rows.length,
    };

    return Promise.resolve(result);
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
    const [maybeSchema, maybeTable] = this.splitOnce(tablePath);

    // Probably need a nicer way to do this...
    const command = maybeTable
      ? `PRAGMA ${maybeSchema}.table_info(${maybeTable})`
      : `PRAGMA table_info(${maybeSchema})`;
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

    // console.debug('structDef', structDef);

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

  private escapeIdentifier(identifier: string): string {
    return `"${identifier.replace(/"/g, '""')}"`;
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

  private splitOnce(str: string): [string, string] | [string] {
    const index = str.indexOf('.');
    return index === -1 ? [str] : [str.slice(0, index), str.slice(index + 1)];
  }
}
