/*
 * Copyright 2023 Google LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files
 * (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import type {QueryBuilder} from '../types/query-builder';
import {MalloyElement} from '../types/malloy-element';
import type {QueryPropertyInterface} from '../types/query-property-interface';
import {DateTime} from 'luxon';

export class TimezoneStatement
  extends MalloyElement
  implements QueryPropertyInterface
{
  elementType = 'timezone';
  forceQueryClass = undefined;
  queryRefinementStage = undefined;
  constructor(readonly tz: string) {
    super();
  }

  get isValid(): boolean {
    try {
      DateTime.fromISO('2020-02-19T00:00:00', {zone: this.tz});
      return true;
    } catch {
      return false;
    }
  }

  queryExecute(executeFor: QueryBuilder) {
    executeFor.resultFS.setTimezone(this.tz);
  }
}
