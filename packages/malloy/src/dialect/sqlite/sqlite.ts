import {values} from 'lodash';
import type {
  TimeLiteralNode,
  Expr,
  LeafAtomicTypeDef,
  AtomicTypeDef,
  RegexMatchExpr,
  TimeExtractExpr,
  ExtractUnit,
  Sampling,
  ArrayLiteralNode,
  MeasureTimeExpr,
  OrderBy,
  RecordLiteralNode,
  TimeDeltaExpr,
  TimeTruncExpr,
  TypecastExpr,
} from '../../model';
import {TD} from '../../model';
import type {
  DialectFieldList,
  DialectField,
  FieldReferenceType,
  QueryInfo,
} from '../dialect';
import {Dialect, qtz} from '../dialect';
import type {DialectFunctionOverloadDef} from '../functions';
import {expandBlueprintMap, expandOverrideMap} from '../functions';
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

export class SqliteDialect extends Dialect {
  name = 'sqlite';
  defaultNumberType = 'INTEGER';
  defaultDecimalType = 'REAL';
  udfPrefix = 'UDF_';
  hasFinalStage = false;
  divisionIsInteger = true; // 50/100 = 0, 50/100.0 = 0.5
  supportsSumDistinctFunction = true;
  unnestWithNumbers = false;
  defaultSampling = {enable: false};
  supportsAggDistinct = true; // TODO
  supportsCTEinCoorelatedSubQueries = false; // TODO
  dontUnionIndex = false; // TODO
  supportsQualify = true; // TODO
  supportsNesting = true; // TODO
  cantPartitionWindowFunctionsOnExpressions = false;
  hasModOperator = true;
  nestedArrays = true; // TODO
  supportsHyperLogLog = false;
  likeEscape = true; // Escape is supported for like via LIKE '^%' ESCAPE '^'
  supportUnnestArrayAgg = true; // TODO
  supportsSafeCast = false; // TODO

  experimental = false; // Remove later, but quiet for now.
  supportsCountApprox = false;

  jsonType = 'JSON'; // Can case to JSONB if needed

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
      case 'timestamp':
        return 'DATETIME';
      case 'date':
        return 'DATE';
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
    const gen = Array(groupSetCount + 1).fill(0);

    return Builder.exprs(
      'CROSS JOIN',
      Builder.groupExpr(
        'SELECT',
        Builder.named('column1', 'group_set'),
        'FROM',
        Builder.literValues(...gen.map((_, v) => [v]))
      )
    );
  }

  sqlTypeToMalloyType(sqlType: string): LeafAtomicTypeDef {
    const baseSqlType = sqlType.match(/^(\w+)/)?.at(0) ?? sqlType;
    const mappedType = sqliteToMallyTypes[baseSqlType.toLocaleLowerCase()];

    if (mappedType) {
      return mappedType;
    }

    // console.error(
    //   `Unknown SQLite type ${sqlType} (${baseSqlType}) - defaulting to sql native`
    // );

    return {
      type: 'sql native',
      rawType: sqlType,
    };
  }

  sqlAnyValue(_groupSet: number, fieldName: string): string {
    return Builder.func('MAX', fieldName);
  }

  sqlRegexpMatch(match: RegexMatchExpr): string {
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
      return Builder.func('DATE', Builder.stringLiteral(lit.literal));
    }

    const tz = lit.timezone || qtz(qi);

    if (tz) {
      // Sqlite doesnt support timedb timezones directly,
      // we'll need to resolve of the offset here :(
      throw new Error('Timezone not supported in SQLite');
    }

    return Builder.func('DATETIME', Builder.stringLiteral(lit.literal));
  }

  sqlNowExpr(): string {
    return 'CURRENT_TIMESTAMP';
  }

  sqlSelectAliasAsStruct(
    alias: string,
    dialectFieldList: DialectFieldList
  ): string {
    const fields = this.mapFieldsForJsonObject(
      dialectFieldList,
      false,
      f => f.sqlOutputName,
      e => `${alias}.${e.sqlOutputName}`
    );

    return Builder.jsonObject(...fields);
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

  sqlAggregateTurtle(
    groupSet: number,
    fieldList: DialectFieldList,
    orderBy: string | undefined,
    limit: number | undefined
  ): string {
    // We take advantage of json_each to get ordering and limiting
    // within json_group_array, we build the turtle, then project
    // it back to rows, then group it again.
    //
    // NOTE: This feels like its relying on undefined behavior,
    // technically the ORDER BY should not work within the grouping
    // aggregate, but it does. :melting-face:

    const subSelectAlias = '_j_turtle_' + groupSet;

    // json_each(json_group_array(json_object(...))) as sub_select
    // Gives a table expression
    let jsonObjectProject = Builder.jsonObject(
      ...this.mapFieldsForJsonObject(fieldList, false, f => f.rawName)
    );

    // If we have an order by we need to apply it to the jsonObjectProject
    // so we get JSON_GROUP_ARRAY(JSON_OBJECT(...) ORDERED BY X DESC)
    if (orderBy) {
      jsonObjectProject += ` ${orderBy}`;
    }
    // Note: we are applying an aggregate filter here to limit to the group set
    const tableExpr = Builder.func(
      'JSON_EACH',
      Builder.exprs(
        Builder.func('JSON_GROUP_ARRAY', jsonObjectProject),
        Builder.aggFilter(Builder.eq('group_set', groupSet))
      )
    );

    // Now we are going to select from the table expression, and re-aggregate
    // json_each value back into an array (!), if there is a limit we can
    // can use key (from json_each) to limit the number of rows;

    const select = Builder.exprs(
      'SELECT',
      Builder.func('JSON_GROUP_ARRAY', `${subSelectAlias}.value`),
      'FROM',
      Builder.named(tableExpr, subSelectAlias),
      typeof limit === 'number'
        ? Builder.where(Builder.binOp('<', `${subSelectAlias}.key`, limit)) // 0 indexed, so lt
        : undefined
    );

    return Builder.groupExpr(select);
  }

  sqlAnyValueTurtle(groupSet: number, fieldList: DialectFieldList): string {
    return Builder.if(
      Builder.eq('group_set', groupSet),
      Builder.jsonObject(...this.mapFieldsForJsonObject(fieldList))
    );
  }

  sqlAnyValueLastTurtle(
    name: string,
    groupSet: number,
    sqlName: string
  ): string {
    const expr = Builder.anyValue(
      Builder.if(
        Builder.and(Builder.eq('group_set', groupSet), Builder.notNull(name)),
        name
      )
    );

    return Builder.named(expr, sqlName);
  }

  sqlCoaleseMeasuresInline(
    groupSet: number,
    fieldList: DialectFieldList
  ): string {
    const fields = this.mapFieldsForJsonObject(fieldList);
    const nullValues = this.mapFieldsForJsonObject(fieldList, true);

    return Builder.coalesce(
      Builder.anyValue(
        Builder.if(
          Builder.eq('group_set', groupSet),
          Builder.jsonObject(...fields)
        )
      ),
      Builder.jsonObject(...nullValues)
    );
  }

  sqlUnnestAlias(
    source: string,
    alias: string,
    fieldList: DialectFieldList,
    needDistinctKey: boolean,
    isArray: boolean,
    isInNestedPipeline: boolean
  ): string {
    // This doesn't work in sqlite as there is no "lateral" support
    // if these could be injected as stages OR correlated subqueries
    // OR if we could understand the chain of CTEs this could be
    // made to work. For now any pipelined stages will be fail.

    // Given a turtle (e.g. a bloc of json) we need to unnest it
    const jsonTable = this.jsonTable(source, fieldList, isArray);

    return Builder.exprs(
      'CROSS JOIN',
      Builder.named(jsonTable, alias),
      'ON TRUE'
    );
  }

  malloyToSQL(t: string) {
    if (t === 'number') {
      return 'DOUBLE';
    } else if (t === 'string') {
      return 'TEXT';
    } else if (t === 'struct' || t === 'array' || t === 'record') {
      return 'TEXT';
    } else return t;
  }

  jsonTable(source: string, fieldList: DialectFieldList, isArray: boolean) {
    // If we have an array, we are selecting the json values within the array
    if (isArray) {
      return Builder.groupExpr(
        'SELECT',
        Builder.list(
          Builder.named('key', '__row_id'),
          Builder.named(Builder.jsonPath('value', '$'), 'value')
        ),
        'FROM',
        Builder.func('JSON_EACH', source)
      );
    }

    // If we have a record, we are selecting the json values within the record

    const fields = fieldList.map(f =>
      Builder.named(
        Builder.cast(
          Builder.jsonPath('value', `$.${f.rawName}`),
          this.malloyToSQL(f.type)
        ),
        f.sqlOutputName
      )
    );

    return Builder.groupExpr(
      'SELECT',
      Builder.list(Builder.named('key', '__row_id'), ...fields),
      'FROM',
      Builder.func('JSON_EACH', source)
    );
  }

  sqlSumDistinctHashedKey(sqlDistinctKey: string): string {
    throw new Error('Method not implemented.');
  }

  sqlFieldReference(
    parentAlias: string,
    parentType: FieldReferenceType,
    childName: string,
    _childType: string
  ): string {
    const child = this.sqlMaybeQuoteIdentifier(childName);
    return `${parentAlias}.${child}`;
  }

  sqlUnnestPipelineHead(
    isSingleton: boolean,
    sourceSQLExpression: string,
    fieldList?: DialectFieldList
  ): string {
    throw new Error('Method not implemented.');
  }

  sqlCreateFunction(id: string, funcText: string): string {
    throw new Error('Method not implemented.');
  }

  sqlCreateFunctionCombineLastStage(
    lastStageName: string,
    fieldList: DialectFieldList,
    orderBy: OrderBy[] | undefined
  ): string {
    throw new Error('Method not implemented.');
  }

  sqlCreateTableAsSelect(tableName: string, sql: string): string {
    throw new Error('Method not implemented.');
  }

  castToString(expression: string): string {
    return Builder.cast(expression, 'TEXT');
  }

  concat(...values: string[]): string {
    return values.join(' || ');
  }

  sqlTruncExpr(qi: QueryInfo, toTrunc: TimeTruncExpr): string {
    throw new Error('Method not implemented.');
  }

  sqlMeasureTimeExpr(e: MeasureTimeExpr): string {
    throw new Error('Method not implemented.');
  }

  sqlAlterTimeExpr(df: TimeDeltaExpr): string {
    throw new Error('Method not implemented.');
  }

  sqlCast(qi: QueryInfo, cast: TypecastExpr): string {
    const {op, srcTypeDef, dstTypeDef, dstSQLType} = this.sqlCastPrep(cast);
    //  TODO:  Time handling
    return cast.e.sql || '';
  }

  sqlLiteralString(literal: string): string {
    const noVirgule = literal.replace(/\\/g, '\\\\');
    return "'" + noVirgule.replace(/'/g, "\\'") + "'";
  }

  sqlLiteralRegexp(literal: string): string {
    const noVirgule = literal.replace(/\\/g, '\\\\');
    return "'" + noVirgule.replace(/'/g, "\\'") + "'";
  }

  sqlLiteralArray(lit: ArrayLiteralNode): string {
    return Builder.func('JSON_ARRAY', ...lit.kids.values.map(v => v.sql));
  }

  sqlLiteralRecord(lit: RecordLiteralNode): string {
    const tuples = Object.entries(lit.kids).map(([key, value]) => {
      return [key, value.sql] as [BuilderExpr, BuilderExpr];
    });

    return Builder.jsonObject(...tuples);
  }

  validateTypeName(sqlType: string): boolean {
    return sqlType.match(/^[A-Za-z\s(),<>0-9]*$/) !== null;
  }

  jsonFunc(func: string, args: (string | undefined)[]): string {
    return `${this.jsonType}_${func}(${args.filter(Boolean).join(',')})`;
  }

  public splitPath(str: string): {db?: string; table: string} {
    const index = str.indexOf('.');
    return index === -1
      ? {table: str}
      : {db: str.slice(0, index), table: str.slice(index + 1)};
  }

  private mapFields(fieldList: DialectFieldList): string {
    return Builder.list(
      ...fieldList.flatMap(f => [
        Builder.stringLiteral(f.rawName),
        f.sqlExpression,
      ])
    );
  }

  private mapFieldsForJsonObject(
    fieldList: DialectFieldList,
    nullValues?: boolean,
    fieldSelector: (f: DialectField) => string = f => f.sqlOutputName,
    exprSelector: (f: DialectField) => string = f => f.sqlExpression
  ): [BuilderExpr, BuilderExpr][] {
    return fieldList.map(f => [
      Builder.stringLiteral(fieldSelector(f)),
      nullValues ? Builder.NULL : exprSelector(f),
    ]);
  }
}

type ExprValue = string | number | boolean | null | undefined;
type BuilderExpr = ExprValue | (() => ExprValue);

class Builder {
  public static readonly NULL = 'NULL';
  public static readonly TRUE = 'TRUE';
  public static readonly FALSE = 'FALSE';
  public static readonly EMPTY_STRING_LITERAL = "''";

  public static expr(expr: BuilderExpr): string {
    if (typeof expr === 'function') {
      return Builder.expr(expr());
    } else if (typeof expr === 'undefined') {
      return '';
    } else if (expr === null) {
      return Builder.NULL;
    }

    return expr.toString();
  }

  public static exprs(...exprs: BuilderExpr[]): string {
    return exprs.map(Builder.expr).join('\n');
  }

  public static stringLiteral(expr: BuilderExpr): string {
    const noVirgule = Builder.expr(expr).replace(/\\/g, '\\\\');
    return "'" + noVirgule.replace(/'/g, "\\'") + "'";
  }

  public static list(...args: BuilderExpr[]): string {
    return args.map(Builder.expr).join(', ');
  }

  public static literValues(...args: BuilderExpr[][]): string {
    return `(VALUES ${Builder.valueList(...args)})`;
  }

  public static valueList(...args: BuilderExpr[][]): string {
    return Builder.list(...args.map(v => Builder.groupExpr(...v)));
  }

  public static func(name: BuilderExpr, ...args: BuilderExpr[]): string {
    return `${Builder.expr(name)}(${Builder.list(...args)})`;
  }

  public static coalesce(...args: BuilderExpr[]): string {
    return Builder.func('COALESCE', ...args);
  }

  public static groupExpr(...args: BuilderExpr[]): string {
    return `(${Builder.exprs(...args)})`;
  }

  public static cast(expr: BuilderExpr, type: string) {
    return `CAST(${Builder.expr(expr)} AS ${type})`;
  }

  public static jsonObject(...args: [BuilderExpr, BuilderExpr][]): string {
    return this.func('JSON_OBJECT', ...args.flatMap(kv => kv));
  }

  public static and(...expr): string {
    return expr.map(Builder.expr).join(' AND ');
  }

  public static or(...expr): string {
    return expr.map(Builder.expr).join(' OR ');
  }

  public static eq(left: BuilderExpr, right: BuilderExpr): string {
    return Builder.binOp('=', left, right);
  }

  public static notNull(expr: BuilderExpr): string {
    return `${Builder.expr(expr)} IS NOT NULL`;
  }

  public static binOp(op: string, left: BuilderExpr, right: BuilderExpr) {
    return `${Builder.expr(left)} ${op} ${Builder.expr(right)}`;
  }

  public static anyValue(expr: BuilderExpr): string {
    return Builder.func('MAX', expr);
  }

  public static named(expr: BuilderExpr, name: string): string {
    return `${Builder.expr(expr)} AS ${name}`;
  }

  public static jsonPath(expr: BuilderExpr, path: string): string {
    return `${Builder.expr(expr)} ->> '${path}'`;
  }

  public static aggFilter(expr: BuilderExpr) {
    return `FILTER (${Builder.where(expr)})`;
  }

  public static where(expr: BuilderExpr) {
    return `WHERE ${Builder.expr(expr)}`;
  }

  public static if(
    condition: BuilderExpr,
    trueValue: BuilderExpr,
    falseValue?: BuilderExpr
  ): string {
    return Builder.func(
      'IIF',
      Builder.expr(condition),
      Builder.expr(trueValue),
      falseValue ? Builder.expr(falseValue) : Builder.NULL
    );
  }
}
