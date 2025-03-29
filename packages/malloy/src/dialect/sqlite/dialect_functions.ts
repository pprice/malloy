import type {
  DefinitionBlueprint,
  DefinitionBlueprintMap,
  OverloadedDefinitionBlueprint,
} from '../functions/util';

const string_reverse: DefinitionBlueprint = {
  takes: {'str': 'string'},
  returns: 'string',
  impl: {sql: 'UDF_REVERSE(CAST(${str} AS VARCHAR))'},
};

const string_agg: OverloadedDefinitionBlueprint = {
  default_separator: {
    takes: {'value': {dimension: 'string'}},
    returns: {measure: 'string'},
    supportsOrderBy: true,
    impl: {
      sql: "GROUP_CONCAT(${value} ${order_by:}, ',')",
    },
  },
  with_separator: {
    takes: {
      'value': {dimension: 'string'},
      'separator': {literal: 'string'},
    },
    returns: {measure: 'string'},
    supportsOrderBy: true,
    impl: {
      sql: 'GROUP_CONCAT(${value} ${order_by:}, ${separator})',
    },
  },
};

// Sqlite doesn't like DISTINCT without an order by for aggregate functions (!)
// To workaround this we use a udf that does a set aggregate
const string_agg_distinct: OverloadedDefinitionBlueprint = {
  default_separator: {
    ...string_agg['default_separator'],
    isSymmetric: true,
    impl: {
      sql: "UDF_SET_CONCAT(${value} ${order_by:}, ',')",
    },
  },
  with_separator: {
    ...string_agg['with_separator'],
    isSymmetric: true,
    impl: {
      sql: 'UDF_SET_CONCAT(${value} ${order_by:}, ${separator})',
    },
  },
};

export const SQLITE_DIALECT_FUNCTIONS: DefinitionBlueprintMap = {
  reverse: string_reverse,
  string_agg,
  string_agg_distinct,
};
