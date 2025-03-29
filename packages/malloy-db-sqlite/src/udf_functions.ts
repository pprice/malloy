import type {RegistrationOptions, Database} from 'better-sqlite3';
import * as node_crypto from 'crypto';

// Types
type ScalarUdf = [string, RegistrationOptions | undefined | null, Function];

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
const SCALAR_UDF_FUNCTIONS: readonly ScalarUdf[] = [
  ['udf_uuid', {deterministic: true}, uuid],
  ['udf_regexp_contains', {deterministic: true}, regexp_contains],
  ['udf_regexp_extract', {deterministic: true}, regexp_extract],
  ['udf_regexp_replace', {deterministic: true}, regexp_replace],
  ['udf_string_repeat', {deterministic: true}, string_repeat],
  ['udf_reverse', {deterministic: true}, reverse],
] as const;

export function registerUserDefinedFunctions(db: Database) {
  const registered: string[] = [];

  // TODO: Make sure we only do this once...
  for (const [name, options, fn] of SCALAR_UDF_FUNCTIONS) {
    db.function(name, {...options}, typeErase(fn));
    registered.push(name);
  }

  return registered;
}

/**
 * Generate a UUID.
 */
function uuid() {
  if (
    typeof crypto === 'undefined' ||
    typeof crypto.randomUUID !== 'function'
  ) {
    return crypto.randomUUID();
  }

  // Node.js crypto module
  return node_crypto.randomUUID();
}

/**
 * regexp_contains implementation, which checks if a string contains a pattern.
 *
 * @param input Input string to search
 * @param pattern  Regular expression pattern to search for
 * @returns True if the pattern is found in the input string, false otherwise
 */
function regexp_contains(
  input: string | null,
  pattern: string | null
): number | null {
  if (isNullOrUndefined(input) || isNullOrUndefined(pattern)) {
    // TODO: Validate if this is expected for a boolean expression
    return null;
  }

  // We need to escape the pattern for use in a regex
  const regex = new RegExp(pattern);
  return regex.test(input) ? 1 : 0;
}

/**
 * regexp_extract implementation, which extracts the first match of a regular expression from a string.
 *
 * @param input Input string to search
 * @param pattern Regular expression pattern to search for
 * @returns The first match of the pattern in the input string, or null if no match is found
 */
function regexp_extract(input: string | null, pattern: string | null) {
  if (isNullOrUndefined(input) || isNullOrUndefined(pattern)) {
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
  if (
    isNullOrUndefined(input) ||
    isNullOrUndefined(pattern) ||
    isNullOrUndefined(replacement)
  ) {
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

/**
 * string_repeat implementation, which repeats a string a specified number of times.
 *
 * @param input Input string to repeat
 * @param count Number of times to repeat the string
 * @returns The repeated string
 */

function string_repeat(input: string | null, count: number | null) {
  if (isNullOrUndefined(input) || isNullOrUndefined(count)) {
    return null;
  }

  // Javascript will honor `0`, but not negative numbers
  if (count < 0) {
    return null;
  }

  return input.repeat(count);
}

/**
 * reverse implementation, which reverses a string.
 *
 * @param input Input string to reverse
 * @returns The reversed string
 */

function reverse(input: string | null) {
  if (isNullOrUndefined(input)) {
    return null;
  }

  // Unicode safe reverse
  return Array.from(input).reverse().join('');
}

/** Helper to infer if T is defined */
function isNullOrUndefined<T>(
  value: T | null | undefined
): value is null | undefined {
  // TODO: Figureout a varadic way to do this
  return value === null || value === undefined;
}

/** Type erasure for functions, keep ts x better-sqlite happy */
function typeErase(fn: Function): (...args: unknown[]) => unknown {
  return fn as (...args: unknown[]) => unknown;
}
