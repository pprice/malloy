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
} from '../../model';
import {
  Dialect,
  DialectFieldList,
  FieldReferenceType,
  qtz,
  QueryInfo,
} from '../dialect';
import {DialectFunctionOverloadDef} from '../functions';
import {StandardSQLDialect} from '../standardsql/standardsql';

export class SqliteDialect extends StandardSQLDialect {
  name = 'sqlite';
  defaultNumberType = 'DOUBLE PRECISION';
  defaultDecimalType = 'DECIMAL';
  experimental = false; // Remove later, but quiet for now.

  constructor() {
    super();
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
}
