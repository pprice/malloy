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
  dbPath: string;
  readonly?: boolean;
  fileMustExist?: boolean;
}

const SQLITE_MIN_VERSION = new semvar.SemVer('3.8.3');

export class SqliteConnection extends BaseConnection {
  public name: string;
  private db: SqliteDatabase.Database;
  private readonly dialet = new StandardSQLDialect();

  constructor(name: string, options: SqliteConnectionOptions) {
    super();
    this.name = name;

    this.db = new SqliteDatabase(options.dbPath, {
      // These are required to be boolean
      readonly: !!options.readonly || false,
      fileMustExist: !!options.fileMustExist || false,
    });

    this.validateMinimumVersion();
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
    const pragmaQuery = `PRAGMA table_info(${this.escapeIdentifier(
      tableName
    )})`;

    const schema = this.db
      .prepare<unknown[], PragmaTableInfo>(pragmaQuery)
      .all();

    const structDef: TableSourceDef = {
      type: 'table',
      name: tableName,
      tablePath,
      dialect: this.dialectName,
      connection: this.name,
      fields: schema.map(c => ({
        ...this.dialet.sqlTypeToMalloyType(c.type),
        name: c.name,
      })),
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

  private escapeIdentifier(identifier: string): string {
    return `"${identifier.replace(/"/g, '""')}"`;
  }
}
