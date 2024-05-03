import { BskyXRPC } from '@mary/bluesky-client';

import { differenceInDays } from 'date-fns/differenceInDays';
import * as v from '@badrap/valita';

import { serializedState, type InstanceInfo, type LabelerInfo, type SerializedState } from '../src/state';

import { PromiseQueue } from '../src/utils/pqueue';

const now = Date.now();

const resultFile = v.string().parse(process.env.RESULT_FILE);
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

// Some schema validations
const pdsDescribeServerResponse = v.object({
	availableUserDomains: v.array(v.string()),
	did: v.string(),

	contact: v.object({ email: v.string().optional() }).optional(),
	inviteCodeRequired: v.boolean().optional(),
	links: v.object({ privacyPolicy: v.string().optional(), termsOfService: v.string().optional() }).optional(),
	phoneVerificationRequired: v.boolean().optional(),
});

const labelerQueryLabelsResponse = v.object({
	cursor: v.string().optional(),
	labels: v.array(
		v.object({
			src: v.string(),
			uri: v.string(),
			val: v.string(),
			cts: v.string(),

			cid: v.string().optional(),
			exp: v.string().optional(),
			neg: v.boolean().optional(),
			sig: v.object({ $bytes: v.string() }).optional(),
			ver: v.number().optional(),
		}),
	),
});

// Global states
const pdses = new Map<string, InstanceInfo>(state ? Object.entries(state.pdses) : []);
const labelers = new Map<string, LabelerInfo>(state ? Object.entries(state.labelers) : []);

const queue = new PromiseQueue();

// Connect to PDSes
console.log(`crawling known pdses`);

const pdsResults = await Promise.all(
	Array.from(pdses, ([href, obj]) => {
		return queue.add(async () => {
			const host = new URL(href).host;
			const rpc = new BskyXRPC({ service: href });

			const signal = AbortSignal.timeout(15_000);
			const meta = await rpc
				.get('com.atproto.server.describeServer', { signal })
				.then(({ data: rawData }) => {
					const data = pdsDescribeServerResponse.parse(rawData, { mode: 'passthrough' });

					if (data.did !== `did:web:${host}`) {
						throw new Error(`did mismatch`);
					}

					return data;
				})
				.catch(() => null);

			if (meta === null) {
				const errorAt = obj.errorAt;

				if (errorAt === undefined) {
					obj.errorAt = now;
				} else if (differenceInDays(now, errorAt) > 7) {
					pdses.delete(href);
				}

				console.log(`  ${host}: fail`);
				return;
			}

			obj.errorAt = undefined;

			console.log(`  ${host}: pass`);
			return { host, meta };
		});
	}),
).then((results) => results.filter((r) => r !== undefined));

// Connect to labelers
console.log(`crawling known labelers`);

const labelerResults = await Promise.all(
	Array.from(labelers, async ([href, obj]) => {
		return queue.add(async () => {
			const host = new URL(href).host;
			const rpc = new BskyXRPC({ service: href });

			const signal = AbortSignal.timeout(15_000);
			const meta = await rpc
				.get('com.atproto.label.queryLabels', { signal: signal, params: { uriPatterns: ['*'], limit: 1 } })
				.then(({ data: rawData }) => labelerQueryLabelsResponse.parse(rawData, { mode: 'passthrough' }))
				.catch(() => null);

			if (meta === null) {
				const errorAt = obj.errorAt;

				if (errorAt === undefined) {
					obj.errorAt = now;
				} else if (differenceInDays(now, errorAt) > 7) {
					labelers.delete(href);
				}

				console.log(`  ${host}: fail`);
				return;
			}

			obj.errorAt = undefined;

			console.log(`  ${host}: pass`);
			return { host };
		});
	}),
).then((results) => results.filter((r) => r !== undefined));

// Markdown stuff
{
	const PDS_RE = /(?<=<!-- pds-start -->)[^]*(?=<!-- pds-end -->)/;
	const LABELER_RE = /(?<=<!-- labeler-start -->)[^]*(?=<!-- labeler-end -->)/;

	const template = `# Scraped AT Protocol instances

Last updated: {{time}}[^1]

Found by enumerating plc.directory and bsky.network, some instances might not be
part of mainnet.

## Personal data servers

<!-- pds-start --><!-- pds-end -->

## Labelers

<!-- labeler-start --><!-- labeler-end -->

[^1]: Reflecting actual changes, not when the scraper was last run
`;

	let pdsTable = `
| PDS | Open? |
| --- | --- |
`;

	let labelerTable = `
| Labeler |
| --- |
`;

	// Generate the PDS table
	for (const { host, meta } of pdsResults) {
		pdsTable += `| ${host} | ${!meta.inviteCodeRequired ? 'Yes' : 'No'} |\n`;
	}

	// Generate the labeler table
	for (const { host } of labelerResults) {
		labelerTable += `| ${host} |\n`;
	}

	// Read existing Markdown file, check if it's equivalent
	let shouldWrite = true;

	try {
		const source = await Bun.file(resultFile).text();

		if (PDS_RE.exec(source)?.[0] === pdsTable && LABELER_RE.exec(source)?.[0] === labelerTable) {
			shouldWrite = false;
		}
	} catch {}

	// Write the markdown file
	if (shouldWrite) {
		const final = template
			.replace('{{time}}', new Date().toISOString())
			.replace(PDS_RE, pdsTable)
			.replace(LABELER_RE, labelerTable);

		await Bun.write(resultFile, final);
		console.log(`wrote to ${resultFile}`);
	} else {
		console.log(`writing skipped`);
	}
}

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
		labelers: Object.fromEntries(Array.from(labelers)),
	};

	await Bun.write(stateFile, JSON.stringify(serialized, null, '\t'));
}
