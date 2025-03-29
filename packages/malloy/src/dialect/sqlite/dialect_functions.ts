import type {
  DefinitionBlueprint,
  DefinitionBlueprintMap,
} from '../functions/util';

const string_reverse: DefinitionBlueprint = {
  takes: {'str': 'string'},
  returns: 'string',
  impl: {sql: 'UDF_REVERSE(CAST(${str} AS VARCHAR))'},
};

export const SQLITE_DIALECT_FUNCTIONS: DefinitionBlueprintMap = {
  reverse: string_reverse,
};
