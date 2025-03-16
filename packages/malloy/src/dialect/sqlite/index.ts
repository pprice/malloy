import {StandardSQLDialect} from '../standardsql';

export class SqliteDialect extends StandardSQLDialect {
  constructor() {
    super();
    this.name = 'sqlite';
  }
}
