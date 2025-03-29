import {
  Sampling,
  OrderBy,
  TimeTruncExpr,
  TimeExtractExpr,
  MeasureTimeExpr,
  TimeDeltaExpr,
  TypecastExpr,
  RegexMatchExpr,
  TimeLiteralNode,
  ArrayLiteralNode,
  RecordLiteralNode,
  LeafAtomicTypeDef,
  AtomicTypeDef,
  TD,
  Expr,
} from '../../model';
import {
  Dialect,
  DialectFieldList,
  FieldReferenceType,
  qtz,
  QueryInfo,
} from '../dialect';
import {DialectFunctionOverloadDef, expandOverrideMap} from '../functions';
import {StandardSQLDialect} from '../standardsql/standardsql';
import {SQLITE_MALLOY_STANDARD_OVERLOADS} from './function_overrides';

export class SqliteDialect extends StandardSQLDialect {
  name = 'sqlite';
  defaultNumberType = 'DOUBLE PRECISION';
  defaultDecimalType = 'DECIMAL';
  experimental = false; // Remove later, but quiet for now.

  constructor() {
    super();
  }

  sqlMaybeQuoteIdentifier(identifier: string): string {
    return identifier;
  }

  quoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  getDialectFunctionOverrides(): {
    [name: string]: DialectFunctionOverloadDef[];
  } {
    return expandOverrideMap(SQLITE_MALLOY_STANDARD_OVERLOADS);
  }

  exprToSQL(qi: QueryInfo, df: Expr): string | undefined {
    // TODO
    return super.exprToSQL(qi, df);
  }

  quoteTablePath(tablePath: string): string {
    // SQLite doesn't require quoting table paths, but we can do it if needed.
    const {db, table} = this.splitPath(tablePath);

    return db
      ? `${this.quoteIdentifier(db)}.${this.quoteIdentifier(table)}`
      : this.quoteIdentifier(table);
  }

  sqlAnyValue(_groupSet: number, fieldName: string): string {
    return `MAX(${fieldName})`;
  }

  sqlLiteralTime(qi: QueryInfo, lit: TimeLiteralNode): string {
    if (TD.isDate(lit.typeDef)) {
      return `DATE('${lit.literal}')`;
    }

    const tz = lit.timezone || qtz(qi);

    if (tz) {
      // Sqlite doesnt support timedb timezones directly,
      // we'll need to resolve of the offset here :(
      throw new Error('Timezone not supported in SQLite');
    }

    return `DATETIME('${lit.literal}')`;
  }

  sqlNowExpr(): string {
    return 'CURRENT_TIMESTAMP';
  }

  public splitPath(str: string): {db?: string; table: string} {
    const index = str.indexOf('.');
    return index === -1
      ? {table: str}
      : {db: str.slice(0, index), table: str.slice(index + 1)};
  }
}
