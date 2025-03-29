import type {RegistrationOptions, Database} from 'better-sqlite3';

type Udf = [string, RegistrationOptions | undefined | null, Function];

/**
 * User-defined functions (UDFs) for SQLite.
 *
 * These are registered at "connect" time and are used to provide compatibility with malloy
 * expectations for functions.
 *
 * Important: These functions should be setup in dialect/sqlite/function_overrides.ts - udf_
 * prefixes have been added for debugging purposes (e.g. udf_foo_bar is obviously a udf)
 *
 * Important: Adding functions to sqlite is straightforward, but ideally if a "pure sql"
 * overide can be used, prefer that (e.g. see round() in function_overrides.ts)
 */
const UDF_FUNCTIONS: readonly Udf[] = [
  ['udf_regexp_extract', {deterministic: true}, regexp_extract],
  ['udf_regexp_replace', {deterministic: true}, regexp_replace],
] as const;

export function registerUserDefinedFunctions(db: Database) {
  // TODO: Make sure we only do this once...
  for (const [name, options, fn] of UDF_FUNCTIONS) {
    db.function(name, options || {}, typeErase(fn));
  }
}

/**
 * regexp_extract implementation, which extracts the first match of a regular expression from a string.
 *
 * @param input Input string to search
 * @param pattern Regular expression pattern to search for
 * @returns The first match of the pattern in the input string, or null if no match is found
 */
function regexp_extract(input: string | null, pattern: string | null) {
  if (!input || !pattern) {
    return null;
  }

  const matches = input.match(new RegExp(pattern));

  if (!matches || matches.length === 0) {
    return null;
  }

  return matches[0];
}

/**
 * regexp_replace implementation, supporting backreference replacement.
 *
 * @param input Input string to search
 * @param pattern  Regular expression pattern to search for
 * @param replacement Replacement string, which can contain backreferences
 * @returns The modified string with replacements made
 */
function regexp_replace(
  input: string | null,
  pattern: string | null,
  replacement: string | null
) {
  if (!input || !pattern || !replacement) {
    return null;
  }

  // Detect if there are any backreferences in the replacement string
  // Oddity; the strings come to use double escaped, e.g. \\0 vs \0;
  const backreferencePattern = /\\\\(\d)/g;
  const backReferencesMode = backreferencePattern.test(replacement);

  // If there are backreferences, we need to use a function to replace
  // the matches with the backreferences
  return input.replace(new RegExp(pattern, 'g'), (match, ...groups) => {
    // Replace backreferences with the corresponding group
    if (!backReferencesMode) {
      return replacement;
    }

    return replacement.replace(backreferencePattern, (_, n) => {
      const index = parseInt(n);
      return index === 0 ? match : groups[index - 1] ?? '';
    });
  });
}

/** Type erasure for functions, keep ts x better-sqlite happy */
function typeErase(fn: Function): (...args: unknown[]) => unknown {
  return fn as (...args: unknown[]) => unknown;
}
