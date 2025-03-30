import type {MalloyStandardFunctionImplementations as OverrideMap} from '../functions/malloy_standard_functions';

function roundWithNegativePrecisionSQL() {
  // SQLite doesn't support negative precision in ROUND, so we need to do a CASE statement
  // to handle it. We can use the fact that ROUND(12.1, -1) is equivalent to
  // ROUND(12.1 / POWER(10, ABS(-1))) * POWER(10, ABS(-1))
  //
  // NOTE: We can't do -${precision} otherwise this leads to a comment token `--`, so let's use ABS
  return (
    'IF(${precision} < 0, ' +
    'ROUND(${value} / POWER(10, ABS(${precision}))) * POWER(10, ABS(${precision})), ' +
    'ROUND(${value}, ${precision})' +
    ')'
  );
}

function truncWithNegativePrecisionSQL() {
  // if(p >= 0, 10^p, 1.0 / 10^abs(p))
  const precision =
    'IF(${precision} >= 0, POW(10, ${precision}), 1.0 / POW(10, ABS(${precision})))';

  return 'CAST(${value} * (' + precision + ') AS INTEGER) / ' + precision;
}

export const SQLITE_MALLOY_STANDARD_OVERLOADS: OverrideMap = {
  ascii: {
    // SQLite doesn't have an ASCII function, but UNICODE is equivalent as it returns the code point of the first character.
    // If we want to improve this then we can do a bounds check on the result and clamp it to the ascii range.
    function: 'UNICODE',
  },
  byte_length: {
    sql: 'LENGTH(CAST(${value} as BLOB))',
  },
  chr: {
    sql: "IF(${value}, '', CHAR(${value}))",
  },
  div: {sql: 'FLOOR(${dividend} / ${divisor})'},
  least: {
    sql: 'MIN(${...values})',
  },
  log: {
    // Base firt, value second
    sql: 'LOG(${base}, ${value})',
  },
  greatest: {
    sql: 'MAX(${...values})',
  },
  ends_with: {
    // Use GLOB matching for starts with, if sqlite encounters a null
    // input it will resolve to null, but we expect a boolean expression
    // so return 0 instead, however concat('*', ${suffix}) will return '*'
    // so another layer of IF is needed to return 0 if the suffix is null
    sql: "IF(${suffix} IS NULL, 0, IFNULL(GLOB(CONCAT('*', ${suffix}), ${value}), 0))",
  },
  regexp_extract: {
    sql: 'UDF_REGEXP_EXTRACT(${value}, ${pattern})',
  },
  rand: {function: 'RANDOM'},
  replace: {
    regular_expression: {
      sql: 'UDF_REGEXP_REPLACE(${value}, ${pattern}, ${replacement})',
    },
  },
  round: {
    to_precision: {sql: roundWithNegativePrecisionSQL()},
  },
  trunc: {
    to_integer: {sql: 'CAST(${value} AS INTEGER)'},
    to_precision: {sql: truncWithNegativePrecisionSQL()},
  },
  starts_with: {
    // See ends_with for the reasoning
    sql: "IF(${prefix} IS NULL, 0, IFNULL(GLOB(CONCAT(${prefix}, '*'), ${value}), 0))",
  },
  string_repeat: {
    sql: 'UDF_STRING_REPEAT(${value}, ${count})',
  },
  strpos: {
    sql: 'INSTR(${test_string}, ${search_string})',
  },
};
