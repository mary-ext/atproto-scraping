import Database from 'bun:sqlite';
import * as fs from 'node:fs';

import { drizzle } from 'drizzle-orm/bun-sqlite';
import { migrate } from 'drizzle-orm/bun-sqlite/migrator';

import * as schema from './schema';

fs.mkdirSync('./store', { recursive: true });
const sqlite = new Database('./store/did.sqlite3');

export const db = drizzle(sqlite, { schema });

migrate(db, { migrationsFolder: './drizzle' });

export { schema };
