import crypto from 'node:crypto';

import { BskyXRPC } from '@mary/bluesky-client';

import * as v from '@badrap/valita';
import { differenceInDays } from 'date-fns/differenceInDays';
import * as tld from 'tldts';

import {
	serializedState,
	type DidWebInfo,
	type InstanceInfo,
	type LabelerInfo,
	type SerializedState,
} from '../src/state';

import { MAX_FAILURE_DAYS, PLC_URL, RELAY_URL } from '../src/constants';
import { didDocument, type DidDocument } from '../src/utils/did';
import { compareString } from '../src/utils/misc';
import { PromiseQueue } from '../src/utils/pqueue';
import { LineBreakStream, TextDecoderStream } from '../src/utils/stream';

const now = Date.now();

const env = v.object({ STATE_FILE: v.string() }).parse(process.env, { mode: 'passthrough' });

let state: SerializedState | undefined;

// Read existing state file
{
	let json: unknown;

	try {
		json = await Bun.file(env.STATE_FILE).json();
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
	console.log(`  starting ${plcCursor || '<root>'}`);

	do {
		const url = `${PLC_URL}/export` + `?count=${limit}` + (after ? `&after=${after}` : '');

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
					const info = pdses.get(pds);

					if (info === undefined) {
						console.log(`  found pds: ${pds}`);
						pdses.set(pds, {});
					} else if (info.errorAt !== undefined) {
						// reset `errorAt` if we encounter this PDS
						console.log(`  found pds: ${pds} (errored)`);
						info.errorAt = undefined;
					}
				}

				if (labeler) {
					const info = labelers.get(labeler);

					if (info === undefined) {
						console.log(`  found labeler: ${labeler}`);
						labelers.set(labeler, { did });
					} else {
						if (info.errorAt !== undefined) {
							// reset `errorAt` if we encounter this labeler
							console.log(`  found labeler: ${labeler} (errored)`);
							info.errorAt = undefined;
						}

						info.did = did;
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

	console.log(`  ending ${plcCursor || '<root>'}`);

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
	const rpc = new BskyXRPC({ service: RELAY_URL });

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

		// safeguard against the relay returning repos: null
		for (const { did } of data.repos || []) {
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

	console.log(`  ending ${firehoseCursor || '<root>'}`);
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

					const text = await res.text();
					const sha256sum = getHash('sha256', text);

					if (obj.hash !== sha256sum) {
						const json = JSON.parse(text);
						const doc = didDocument.parse(json, { mode: 'passthrough' });

						const pds = getPdsEndpoint(doc);
						const labeler = getLabelerEndpoint(doc);

						console.log(`  ${did}: pass (updated)`);

						if (pds) {
							const info = pdses.get(pds);

							if (info === undefined) {
								console.log(`    found pds: ${pds}`);
								pdses.set(pds, {});
							} else if (info.errorAt !== undefined) {
								// reset `errorAt` if we encounter this PDS
								console.log(`    found pds: ${pds} (errored)`);
								info.errorAt = undefined;
							}
						}

						if (labeler) {
							const info = labelers.get(labeler);

							if (info === undefined) {
								console.log(`    found labeler: ${labeler}`);
								labelers.set(labeler, { did });
							} else {
								if (info.errorAt !== undefined) {
									// reset `errorAt` if we encounter this labeler
									console.log(`    found labeler: ${labeler} (errored)`);
									info.errorAt = undefined;
								}

								info.did = did;
							}
						}

						obj.hash = sha256sum;

						obj.pds = pds;
						obj.labeler = labeler;
					} else {
						console.log(`  ${did}: pass`);
					}

					obj.errorAt = undefined;
				} catch (err) {
					const errorAt = obj.errorAt;

					if (errorAt === undefined) {
						obj.errorAt = now;
					} else if (differenceInDays(now, errorAt) > MAX_FAILURE_DAYS) {
						// It's been days without a response, stop tracking.

						didWebs.delete(did);
					}

					console.log(`  ${did}: fail (${err})`);
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

	await Bun.write(env.STATE_FILE, JSON.stringify(serialized, null, '\t'));
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

function getHash(algo: string, data: string) {
	const hasher = crypto.createHash(algo);
	hasher.update(data);

	return hasher.digest('base64url');
}
