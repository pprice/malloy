import type {
  TimeLiteralNode,
  Expr,
  LeafAtomicTypeDef,
  AtomicTypeDef,
  RegexMatchExpr,
  TimeExtractExpr,
  ExtractUnit,
} from '../../model';
import {TD} from '../../model';
import type {QueryInfo} from '../dialect';
import {qtz} from '../dialect';
import type {DialectFunctionOverloadDef} from '../functions';
import {expandBlueprintMap, expandOverrideMap} from '../functions';
import {StandardSQLDialect} from '../standardsql/standardsql';
import {SQLITE_DIALECT_FUNCTIONS} from './dialect_functions';
import {SQLITE_MALLOY_STANDARD_OVERLOADS} from './function_overrides';

/**
 * https://www.sqlite.org/datatype3.html
 * Note: Technically in sqlite there are only 5 data types:
 *
 * - INTERGER - Arbitrary precision integer
 * - REAL - 8 byte floating point number
 * - TEXT - String
 * - BLOB - Binary data
 * - NUMERIC - Arbitrary precision number, storage as TEXT, REAL or INTEGER
 *
 * However, sqlite is very flexible with types and will map well
 * known types to the above types; e.g. VARCHAR, DECIMAL, etc.
 *
 * When decribing a table, sqlite we get the type that was used
 * to create the table, not the type that sqlite uses to store
 * the data.
 *
 */
const sqliteToMallyTypes: Record<string, LeafAtomicTypeDef> = {
  // Core types
  'text': {type: 'string'},
  'integer': {type: 'number', numberType: 'integer'},
  'real': {type: 'number', numberType: 'float'},
  'numeric': {type: 'number', numberType: 'float'},

  // Common affinities
  // Integer
  'int': {type: 'number', numberType: 'integer'},
  'bigint': {type: 'number', numberType: 'integer'},
  'tinyint': {type: 'number', numberType: 'integer'},
  'smallint': {type: 'number', numberType: 'integer'},
  'mediumint': {type: 'number', numberType: 'integer'},
  'int2': {type: 'number', numberType: 'integer'},
  'int8': {type: 'number', numberType: 'integer'},
  'unsigned big int': {type: 'number', numberType: 'integer'},
  // Real
  'float': {type: 'number', numberType: 'float'},
  'double': {type: 'number', numberType: 'float'},
  'double precision': {type: 'number', numberType: 'float'},
  // Numeric
  'decimal': {type: 'number', numberType: 'float'},
  // String
  'varchar': {type: 'string'},
  'nvarchar': {type: 'string'},
  'nchar': {type: 'string'},
  'clob': {type: 'string'},
  'native character': {type: 'string'},
  'varying character': {type: 'string'},
  // Date time (not really, but we need this for malloy)
  'datetime': {type: 'timestamp'},
  'date': {type: 'date'},
  'boolean': {type: 'boolean'},
};

type StringMapFn = (from: string) => string;

function makeStrfTimeCast(format: string): StringMapFn {
  return (from: string) => `CAST(strftime('${format}', ${from}) as INTEGER)`;
}

/**
 * Timestamp extraction map, malloy unit to strftime
 */
const sqliteTimeExtractMap: Record<ExtractUnit, StringMapFn> = {
  'day': makeStrfTimeCast('%d'),
  'hour': makeStrfTimeCast('%H'),
  'minute': makeStrfTimeCast('%M'),
  'month': makeStrfTimeCast('%m'),
  'second': makeStrfTimeCast('%S'),
  'week': makeStrfTimeCast('%W'),
  'year': makeStrfTimeCast('%Y'),
  'day_of_week': makeStrfTimeCast('%u'), // ISO 1-7
  'day_of_year': makeStrfTimeCast('%j'),
  // Special case, we need to get the month then divide by 3 offset by 1
  'quarter': (from: string) => {
    return `((CAST(strftime('%m', ${from}) as INTEGER) - 1) / 3) + 1`;
  },
};

export class SqliteDialect extends StandardSQLDialect {
  name = 'sqlite';
  defaultNumberType = 'INTEGER';
  defaultDecimalType = 'REAL';
  experimental = false; // Remove later, but quiet for now.
  cantPartitionWindowFunctionsOnExpressions = false;
  supportsCountApprox = false;
  supportsHyperLogLog = false;
  supportsSumDistinctFunction = true;

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

  getDialectFunctions(): {[name: string]: DialectFunctionOverloadDef[]} {
    return expandBlueprintMap(SQLITE_DIALECT_FUNCTIONS);
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

  malloyTypeToSQLType(malloyType: AtomicTypeDef): string {
    switch (malloyType.type) {
      case 'string':
        return 'TEXT';
      case 'number':
        return malloyType.numberType === 'integer' ? 'INTEGER' : 'REAL';
      case 'boolean':
        return 'BOOLEAN';
      default:
        return malloyType.type.toUpperCase();
    }
  }

  sqlGenerateUUID(): string {
    // SQLite doesn't have a built-in UUID function, so, we use a udf...
    return 'UDF_UUID()';
  }

  sqlGroupSetTable(groupSetCount: number): string {
    // SQLite doesn't have a built-in GROUP SET, but VALUES() can be used
    // to get the same effect.

    // Generates a list of values in the form (0), (1), (2), ... (n)
    const valueList = Array(groupSetCount + 1)
      .fill(0)
      .map((_, i) => `(${i})`)
      .join(', ');

    // column1 is automatically generated by sqlite
    return `CROSS JOIN (SELECT column1 as group_set FROM (VALUES ${valueList}))`;
  }

  sqlTypeToMalloyType(sqlType: string): LeafAtomicTypeDef {
    const baseSqlType = sqlType.match(/^(\w+)/)?.at(0) ?? sqlType;
    const mappedType = sqliteToMallyTypes[baseSqlType.toLocaleLowerCase()];

    if (mappedType) {
      return mappedType;
    }

    console.error(
      `Unknown SQLite type ${sqlType} (${baseSqlType}) - defaulting to sql native`
    );

    return {
      type: 'sql native',
      rawType: sqlType,
    };
  }

  sqlAnyValue(_groupSet: number, fieldName: string): string {
    return `MAX(${fieldName})`;
  }

  sqlRegexpMatch(match: RegexMatchExpr): string {
    // SQLite doesn't have a built-in REGEXP function, so, we use a udf...
    return `UDF_REGEXP_CONTAINS(${match.kids.expr.sql}, ${match.kids.regex.sql})`;
  }

  sqlSumDistinct(key: string, value: string, funcName: string): string {
    return `UDF_${funcName}_DISTINCT_PAIRS(${key}, ${value})`;
  }

  sqlStringAggDistinct(
    distinctKey: string,
    valueSQL: string,
    separatorSQL: string
  ): string {
    const keyStart = '__STRING_AGG_KS__';
    const keyEnd = '__STRING_AGG_KE__';
    const distinctValueSQL = `concat('${keyStart}', ${distinctKey}, '${keyEnd}', ${valueSQL})`;
    return `UDF_REGEXP_REPLACE(
      UDF_SET_CONCAT(${distinctValueSQL}${
        separatorSQL.length > 0 ? ',' + separatorSQL : ''
      }),
      '${keyStart}.*?${keyEnd}',
      ''
    )`;
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

  sqlTimeExtractExpr(qi: QueryInfo, from: TimeExtractExpr): string {
    // TODO: As SQLite doesn't have a true datetime type, we need to
    // convert the datetime to a string and then extract when needed...
    const format = sqliteTimeExtractMap[from.units];
    const extractFrom = from.e.sql;

    if (!format) {
      throw new Error(`Unsupported time extract unit ${from.units}`);
    } else if (!extractFrom) {
      throw new Error(`Unsupported time extract expression ${from.e}`);
    }

    return format(extractFrom);
  }

  public splitPath(str: string): {db?: string; table: string} {
    const index = str.indexOf('.');
    return index === -1
      ? {table: str}
      : {db: str.slice(0, index), table: str.slice(index + 1)};
  }
}
