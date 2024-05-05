import { BskyXRPC } from '@mary/bluesky-client';
import type { At } from '@mary/bluesky-client/lexicons';
import { XRPCError } from '@mary/bluesky-client/xrpc';

import * as v from '@badrap/valita';

import { serializedState, type SerializedState } from '../src/state';

import { RELAY_URL } from '../src/constants';
import { PromiseQueue } from '../src/utils/pqueue';

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

const pdses = new Map(state ? Object.entries(state.pdses) : []);

const queue = new PromiseQueue();

const relay = new BskyXRPC({ service: RELAY_URL });

await Promise.all(
	Array.from(pdses, ([href, obj]) => {
		if (obj.errorAt !== undefined) {
			return;
		}

		return queue.add(async () => {
			const host = new URL(href).host;
			const rpc = new BskyXRPC({ service: href });

			let dids: At.DID[];

			try {
				const { data: pdsData } = await rpc.get('com.atproto.sync.listRepos', { params: { limit: 1_000 } });
				const repos = pdsData.repos;

				if (repos.length === 0) {
					console.log(`${host} returned 0 repositories`);
					return;
				}

				shuffle(repos);
				dids = repos.slice(0, 10).map((repo) => repo.did);
			} catch (err) {
				if (err instanceof XRPCError && err.status === 403) {
					console.log(`${host}: fail`);
					pdses.delete(href);
					return;
				}

				console.log(`${host}: unknown error`);
				return;
			}

			for (const did of dids) {
				try {
					console.log(`${host}: testing ${did}`);
					await relay.get('com.atproto.sync.getLatestCommit', { params: { did } });

					console.log(`${host}: pass`);
					return;
				} catch (err) {
					if (err instanceof XRPCError && err.status !== 404) {
						console.log(`${host}: unknown error`);
						return;
					}
				}
			}

			console.log(`${host}: fail`);
			pdses.delete(href);
		});
	}),
);

// Persist the state
{
	const serialized: SerializedState = {
		firehose: {
			cursor: state?.firehose.cursor,
			didWebs: state?.firehose.didWebs || {},
		},
		plc: {
			cursor: state?.plc.cursor,
		},

		pdses: Object.fromEntries(Array.from(pdses)),
		labelers: state?.labelers || {},
	};

	await Bun.write(env.STATE_FILE, JSON.stringify(serialized, null, '\t'));
}

function shuffle(array: any[]) {
	for (let i = array.length - 1; i > 0; i--) {
		const j = Math.floor(Math.random() * (i + 1));
		const temp = array[i];

		array[i] = array[j];
		array[j] = temp;
	}
}
