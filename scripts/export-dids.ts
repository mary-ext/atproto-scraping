import { BskyXRPC } from '@mary/bluesky-client';

import { differenceInDays } from 'date-fns/differenceInDays';
import * as tld from 'tldts';
import * as v from '@badrap/valita';

import {
	serializedState,
	type DidWebInfo,
	type InstanceInfo,
	type LabelerInfo,
	type SerializedState,
} from '../src/state';

import { didDocument, type DidDocument } from '../src/utils/did';
import { PromiseQueue } from '../src/utils/pqueue';
import { LineBreakStream, TextDecoderStream } from '../src/utils/stream';
import { compareString } from '../src/utils/misc';

const now = Date.now();

const stateFile = v.string().parse(process.env.STATE_FILE);
let state: SerializedState | undefined;

// Read existing state file
{
	let json: unknown;

	try {
		json = await Bun.file(stateFile).json();
	} catch {}

	if (json !== undefined) {
		state = serializedState.parse(json);
	}
}

// Global states
const didWebs = new Map<string, DidWebInfo>(state ? Object.entries(state.firehose.didWebs) : []);
const pdses = new Map<string, InstanceInfo>(state ? Object.entries(state.pdses) : []);
const labelers = new Map<string, LabelerInfo>(state ? Object.entries(state.labelers) : []);

const queue = new PromiseQueue();

let plcCursor: string | undefined = state?.plc.cursor;
let firehoseCursor: string | undefined = state?.firehose.cursor;

// Iterate through PLC events
{
	const limit = 1000;
	let after: string | undefined = plcCursor;

	console.log(`crawling plc.directory`);
	console.log(`  starting ${after || '<root>'}`);

	do {
		const url = `https://plc.directory/export` + `?count=${limit}` + (after ? `&after=${after}` : '');

		const response = await get(url);
		const stream = response.body!.pipeThrough(new TextDecoderStream()).pipeThrough(new LineBreakStream());

		after = undefined;

		let count = 0;

		for await (const raw of stream) {
			const json = JSON.parse(raw) as ExportEntry;
			const { did, operation, createdAt } = json;

			if (operation.type === 'plc_operation') {
				const pds = getEndpoint(operation.services.atproto_pds?.endpoint);
				const labeler = getEndpoint(operation.services.atproto_labeler?.endpoint);

				if (pds) {
					if (!pdses.has(pds)) {
						console.log(`  found pds: ${pds}`);
						pdses.set(pds, {});
					}
				}

				if (labeler) {
					if (!labelers.has(labeler)) {
						console.log(`  found labeler: ${labeler}`);
						labelers.set(labeler, { did });
					} else {
						labelers.get(labeler)!.did = did;
					}
				}
			}

			count++;
			after = createdAt;
		}

		if (after) {
			plcCursor = after;
		}

		if (count < limit) {
			break;
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

// Iterate through firehose' known repositories
{
	const rpc = new BskyXRPC({ service: 'https://bsky.network' });

	let cursor: string | undefined = firehoseCursor;

	console.log(`crawling bsky.network`);
	console.log(`  starting ${cursor || '<root>'}`);

	do {
		const { data } = await rpc.get('com.atproto.sync.listRepos', {
			params: {
				cursor: cursor,
				limit: 1_000,
			},
		});

		for (const { did } of data.repos) {
			if (did.startsWith('did:web:')) {
				if (!didWebs.has(did)) {
					console.log(`  found ${did}`);
					didWebs.set(did, {});
				}
			}
		}

		cursor = data.cursor;

		if (cursor) {
			firehoseCursor = cursor;
		}
	} while (cursor !== undefined);
}

// Retrieve PDS information from known did:web identities
{
	console.log(`crawling known did:web identities`);
	const dids = Array.from(didWebs.keys());

	await Promise.all(
		dids.map((did) => {
			return queue.add(async () => {
				const host = did.slice(8);
				const obj = didWebs.get(did)!;

				try {
					const signal = AbortSignal.timeout(15_000);
					const res = await get(`https://${host}/.well-known/did.json`, signal);

					const json = (await res.json()) as unknown;
					const doc = didDocument.parse(json, { mode: 'passthrough' });

					const pds = getPdsEndpoint(doc);
					const labeler = getLabelerEndpoint(doc);

					console.log(`  ${did}: pass`);

					if (pds && obj.pds !== pds) {
						if (!pdses.has(pds)) {
							console.log(`    found pds: ${pds}`);
							pdses.set(pds, {});
						}
					}

					if (labeler && obj.labeler !== labeler) {
						if (!labelers.has(labeler)) {
							console.log(`    found labeler: ${labeler}`);
							labelers.set(labeler, { did });
						} else {
							labelers.get(labeler)!.did = did;
						}
					}

					obj.errorAt = undefined;
					obj.pds = pds;
					obj.labeler = labeler;
				} catch (err) {
					const errorAt = obj.errorAt;

					if (errorAt === undefined) {
						obj.errorAt = now;
					} else if (differenceInDays(now, errorAt) > 7) {
						didWebs.delete(did);
					}

					console.log(`  ${did}: fail`);
				}
			});
		}),
	);
}

// Persist the state
{
	const serialized: SerializedState = {
		firehose: {
			cursor: firehoseCursor,
			didWebs: Object.fromEntries(Array.from(didWebs).sort(([aDid], [bDid]) => compareString(aDid, bDid))),
		},
		plc: {
			cursor: plcCursor,
		},

		pdses: Object.fromEntries(Array.from(pdses).sort(([aHref], [bHref]) => compareString(aHref, bHref))),
		labelers: Object.fromEntries(
			Array.from(labelers).sort(([aHref], [bHref]) => compareString(aHref, bHref)),
		),
	};

	await Bun.write(stateFile, JSON.stringify(serialized, null, '\t'));
}

async function get(url: string, signal?: AbortSignal): Promise<Response> {
	const response = await fetch(url, { signal });

	if (response.status === 429) {
		const delay = 90_000;

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

function getPdsEndpoint(doc: DidDocument): string | undefined {
	return getServiceEndpoint(doc, '#atproto_pds', 'AtprotoPersonalDataServer');
}

function getLabelerEndpoint(doc: DidDocument): string | undefined {
	return getServiceEndpoint(doc, '#atproto_labeler', 'AtprotoLabeler');
}

function getServiceEndpoint(doc: DidDocument, serviceId: string, serviceType: string): string | undefined {
	const did = doc.id;

	const didServiceId = did + serviceId;
	const found = doc.service?.find((service) => service.id === serviceId || service.id === didServiceId);

	if (!found || found.type !== serviceType || typeof found.serviceEndpoint !== 'string') {
		return undefined;
	}

	return getEndpoint(found.serviceEndpoint);
}
function getEndpoint(urlStr: string | undefined): string | undefined {
	if (urlStr === undefined) {
		return undefined;
	}

	// @ts-expect-error
	const url = URL.parse(urlStr) as URL | null;

	if (!url || !(url.protocol === 'http:' || url.protocol === 'https:')) {
		return undefined;
	}

	const parsed = tld.parse(url.hostname);
	if (!parsed.domain || !(parsed.isIcann || parsed.isIp)) {
		return undefined;
	}

	return url.href;
}
