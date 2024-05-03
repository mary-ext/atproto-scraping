import { and, eq, isNotNull, sql } from 'drizzle-orm';
import * as tld from 'tldts';

import { BskyXRPC } from '@mary/bluesky-client';

import { db, schema } from '../src/database';
import { PromiseQueue } from '../src/utils/pqueue';

const template = `# Crawled AT Protocol PDS

Last updated: {{time}}

<!-- table-start --><!-- table-end -->
`;

let table = `
| PDS | User count | Invite required? |
| --- | --- | --- |
`;

const queue = new PromiseQueue();

const pdses = db
	.select({ host: schema.dids.pds, count: sql<number>`count(pds)` })
	.from(schema.dids)
	.where(and(eq(schema.dids.deactivated, false), isNotNull(schema.dids.pds)))
	.groupBy(schema.dids.pds)
	.orderBy(schema.dids.pds)
	.all();

console.log(`got ${pdses.length} result`);

const result = await Promise.all(
	pdses.map(({ host, count }) => {
		return queue.add(async () => {
			if (host === null) {
				return null;
			}

			// @ts-expect-error
			const url = URL.parse(host) as URL | null;
			const parsed = tld.parse(host);

			if (!url || !parsed.domain || !(parsed.isIcann || parsed.isIp)) {
				return null;
			}

			const rpc = new BskyXRPC({ service: host });
			const resp = await rpc
				.get('com.atproto.server.describeServer', { signal: AbortSignal.timeout(10_000) })
				.catch(() => null);

			if (resp === null || typeof resp.data !== 'object') {
				console.log(`${url.host} unreachable`);
				return null;
			}

			if (resp.data.did !== `did:web:${url.host}`) {
				console.log(`${url.host} returned the wrong did`);
				return null;
			}

			console.log(`${url.host} reachable`);
			return { url, count, meta: resp.data };
		});
	}),
);

for (const { url, count, meta } of result.filter((v) => v !== null)) {
	table += `| ${url.host} | ${count} | ${meta.inviteCodeRequired ? 'Yes' : 'No'} |\n`;
}

const TABLE_RE = /(?<=<!-- table-start -->)[^]*(?=<!-- table-end -->)/;
const file = `./README.md`;
let shouldWrite = true;

// Read existing Markdown file, check if it's equivalent to what we have currently
try {
	const source = await Bun.file(file).text();
	const match = TABLE_RE.exec(source);

	if (match && match[0] === table) {
		shouldWrite = false;
	}
} catch {}

if (shouldWrite) {
	const final = template.replace('{{time}}', new Date().toISOString()).replace(TABLE_RE, table);

	await Bun.write(file, final);
	console.log(`wrote to ${file}`);
} else {
	console.log(`writing skipped`);
}
