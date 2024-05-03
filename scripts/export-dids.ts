import { BskyXRPC } from '@mary/bluesky-client';

import { desc, eq, sql } from 'drizzle-orm';
import type { SQLiteInsertValue } from 'drizzle-orm/sqlite-core';

import { db, schema } from '../src/database';
import { PromiseQueue } from '../src/utils/pqueue';
import { LineBreakStream, TextDecoderStream } from '../src/utils/stream';

const queue = new PromiseQueue();

async function get(url: string, signal?: AbortSignal): Promise<Response> {
	const response = await fetch(url, { signal });

	if (response.status === 429) {
		const headers = response.headers;
		const retryAfter = headers.get('retry-after');

		let delay = 10_000;

		if (retryAfter) {
			if (/^\d+$/.test(retryAfter)) {
				delay = +retryAfter;
			} else {
				const date = new Date(retryAfter);

				if (Number.isNaN(date.getTime())) {
					const delta = date.getTime() - Date.now();

					if (delta > 0) {
						delay = delta;
					}
				}
			}
		}

		console.log(`[-] ratelimited, waiting ${delay} ms`);
		await sleep(delay);
		return get(url, signal);
	}

	if (!response.ok) {
		throw new Error(`got ${response.status} from ${url}`);
	}

	return response;
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

// Retrieve PLC DIDs
{
	let after: string | undefined;

	if (true) {
		after = db
			.select({ ts: schema.dids.ts })
			.from(schema.dids)
			.orderBy(desc(schema.dids.ts))
			.get()
			?.ts?.toISOString();
	}

	console.log(`retrieving did:plc from plc.directory`);

	do {
		console.log(`  fetching ${after || '<root>'}`);

		const url = `https://plc.directory/export` + `?count=1000` + (after ? `&after=${after}` : '');

		const response = await get(url);
		const stream = response.body!.pipeThrough(new TextDecoderStream()).pipeThrough(new LineBreakStream());

		after = undefined;

		const values: SQLiteInsertValue<typeof schema.dids>[] = [];

		for await (const raw of stream) {
			const json = JSON.parse(raw) as ExportEntry;
			const { did, operation, nullified, createdAt } = json;

			const ts = new Date(createdAt);

			if (!nullified) {
				if (operation.type === 'plc_operation') {
					const pds = validateUrl(operation.services.atproto_pds?.endpoint);
					values.push({ method: schema.DidMethod.PLC, did, ts, pds });
				} else if (operation.type === 'plc_tombstone') {
					values.push({ method: schema.DidMethod.PLC, did, ts, deactivated: true });
				}
			}

			after = createdAt;
		}

		console.log(`    got ${values.length} values`);

		for (const chunk of chunked(values, 500)) {
			db.insert(schema.dids)
				.values(chunk)
				.onConflictDoUpdate({
					target: schema.dids.did,
					set: { deactivated: sql`excluded.deactivated`, pds: sql`excluded.pds`, ts: sql`excluded.ts` },
					setWhere: sql`excluded.ts > dids.ts`,
				})
				.run();
		}
	} while (after !== undefined);

	interface ExportEntry {
		did: string;
		operation: PlcOperation;
		cid: string;
		nullified: boolean;
		createdAt: string;
	}

	type PlcOperation = LegacyGenesisOp | OperationOp | TombstoneOp;

	interface OperationOp {
		type: 'plc_operation';
		/** did:key[] */
		rotationKeys: string[];
		/** Record<string, did:key> */
		verificationMethods: Record<string, string>;
		alsoKnownAs: string[];
		services: Record<string, Service>;
		prev: string | null;
		sig: string;
	}

	interface TombstoneOp {
		type: 'plc_tombstone';
		prev: string;
		sig: string;
	}

	interface LegacyGenesisOp {
		type: 'create';
		/** did:key */
		signingKey: string;
		/** did:key */
		recoveryKey: string;
		handle: string;
		service: string;
		prev: string | null;
		sig: string;
	}

	interface Service {
		type: string;
		endpoint: string;
	}
}

// Update existing Web DIDs
{
	const limit = 1000;
	let offset = 0;

	console.log(`updating existing web dids`);

	while (true) {
		const results = db
			.select({ did: schema.dids.did })
			.from(schema.dids)
			.where(eq(schema.dids.method, schema.DidMethod.WEB))
			.orderBy(schema.dids.did)
			.limit(limit)
			.offset(offset)
			.all();

		offset += limit;

		if (results.length === 0) {
			break;
		}

		const values: SQLiteInsertValue<typeof schema.dids>[] = [];

		await Promise.all(
			results.map(({ did }) => {
				return queue.add(async () => {
					const ts = new Date();

					try {
						const host = did.slice(8);
						console.log(`  connecting to ${host}`);

						const signal = AbortSignal.timeout(10_000);
						const res = await get(`http://${host}/.well-known/did.json`, signal);

						const json = (await res.json()) as DidDocument;
						const pds = getPdsEndpoint(json);

						values.push({ method: schema.DidMethod.WEB, did, pds, ts });
					} catch {
						values.push({ method: schema.DidMethod.WEB, did, pds: null, ts });
					}
				});
			}),
		);

		for (const chunk of chunked(values, 500)) {
			db.insert(schema.dids)
				.values(chunk)
				.onConflictDoUpdate({
					target: schema.dids.did,
					set: { pds: sql`excluded.pds`, ts: sql`excluded.ts` },
					setWhere: sql`excluded.pds != dids.pds`,
				})
				.run();
		}
	}
}

// Retrieve Web DIDs
{
	const cursorKey = 'relay_cursor';
	const rpc = new BskyXRPC({ service: 'https://bsky.network' });

	let cursor: string | undefined;

	console.log(`retrieving did:web from bsky.network`);

	if (true) {
		const res = db.select().from(schema.configs).where(eq(schema.configs.key, cursorKey)).get();

		if (res) {
			cursor = res.value;
		}
	}

	do {
		console.log(`  fetching ${cursor || '<root>'}`);
		const { data } = await rpc.get('com.atproto.sync.listRepos', {
			params: {
				cursor: cursor,
				limit: 1_000,
			},
		});

		cursor = data.cursor;

		const values: SQLiteInsertValue<typeof schema.dids>[] = [];

		await Promise.all(
			data.repos.map(({ did }) => {
				if (did.startsWith('did:web:')) {
					return queue.add(async () => {
						const ts = new Date();

						try {
							const host = did.slice(8);
							console.log(`  connecting to ${host}`);

							const signal = AbortSignal.timeout(10_000);
							const res = await get(`http://${host}/.well-known/did.json`, signal);

							const json = (await res.json()) as DidDocument;
							const pds = getPdsEndpoint(json);

							values.push({ method: schema.DidMethod.WEB, did, pds, ts });
						} catch {
							values.push({ method: schema.DidMethod.WEB, did, pds: null, ts });
						}
					});
				}
			}),
		);

		if (values.length > 0) {
			console.log(`    got ${values.length} values`);
		}

		for (const chunk of chunked(values, 500)) {
			db.insert(schema.dids)
				.values(chunk)
				.onConflictDoUpdate({
					target: schema.dids.did,
					set: { pds: sql`excluded.pds`, ts: sql`excluded.ts` },
					setWhere: sql`excluded.pds != dids.pds`,
				})
				.run();
		}

		if (cursor) {
			db.insert(schema.configs)
				.values([{ key: cursorKey, value: cursor }])
				.onConflictDoUpdate({
					target: schema.configs.key,
					set: { value: sql`excluded.value` },
				})
				.run();
		}
	} while (cursor !== undefined);
}

console.log(`done`);

function chunked<T>(arr: T[], size: number): T[][] {
	const chunks: T[][] = [];

	for (let i = 0, il = arr.length; i < il; i += size) {
		chunks.push(arr.slice(i, i + size));
	}

	return chunks;
}

function getPdsEndpoint(doc: DidDocument): string | undefined {
	return getServiceEndpoint(doc, '#atproto_pds', 'AtprotoPersonalDataServer');
}

function getServiceEndpoint(doc: DidDocument, serviceId: string, serviceType: string): string | undefined {
	const did = doc.id;

	const didServiceId = did + serviceId;
	const found = doc.service?.find((service) => service.id === serviceId || service.id === didServiceId);

	if (!found || found.type !== serviceType || typeof found.serviceEndpoint !== 'string') {
		return undefined;
	}

	return validateUrl(found.serviceEndpoint);
}
function validateUrl(urlStr: string | undefined): string | undefined {
	if (urlStr === undefined) {
		return undefined;
	}

	// @ts-expect-error
	const url = URL.parse(urlStr) as URL | null;

	if (url === null) {
		return undefined;
	}

	const proto = url.protocol;

	if (url.hostname && (proto === 'http:' || proto === 'https:')) {
		return url.href;
	}
}

interface DidDocument {
	id: string;
	alsoKnownAs?: string[];
	verificationMethod?: DidVerificationMethod[];
	service?: DidService[];
}

interface DidVerificationMethod {
	id: string;
	type: string;
	controller: string;
	publicKeyMultibase?: string;
}

interface DidService {
	id: string;
	type: string;
	serviceEndpoint: string | Record<string, unknown>;
}

declare global {
	interface ReadableStream<R = any> {
		[Symbol.asyncIterator](): AsyncIterableIterator<R>;
	}
}
