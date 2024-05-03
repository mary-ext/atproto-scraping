import { integer, sqliteTable, text } from 'drizzle-orm/sqlite-core';

export const enum DidMethod {
	PLC = 0,
	WEB = 1,
}

export const dids = sqliteTable('dids', {
	did: text('did').primaryKey(),
	method: integer('method').notNull().$type<DidMethod>(),
	deactivated: integer('deactivated', { mode: 'boolean' }).default(false),
	pds: text('pds'),
	ts: integer('ts', { mode: 'timestamp' }),
});

export const configs = sqliteTable('configs', {
	key: text('key').primaryKey(),
	value: text('value').notNull(),
});
