import * as v from '@badrap/valita';

import { serializedState, type SerializedState } from '../src/state';

const env = v
	.object({ STATE_FILE: v.string(), RESULT_FILE: v.string() })
	.parse(process.env, { mode: 'passthrough' });

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

// Markdown stuff
{
	const PDS_RE = /(?<=<!-- pds-start -->)[^]*(?=<!-- pds-end -->)/;
	const LABELER_RE = /(?<=<!-- labeler-start -->)[^]*(?=<!-- labeler-end -->)/;

	const template = `# Scraped AT Protocol instances

Last updated: {{time}}[^1]

Found by enumerating plc.directory and bsky.network, some instances might not be
part of mainnet.

Instances that have not been active for more than 7 days gets dropped off from this list.

## Personal data servers

{{pdsSummary}}

<!-- pds-start --><!-- pds-end -->

## Labelers

{{labelerSummary}}

<!-- labeler-start --><!-- labeler-end -->

[^1]: Reflecting actual changes, not when the scraper was last run
`;

	let pdsTable = `
| PDS | Open? | Version |
| --- | --- | --- |
`;

	let labelerTable = `
| Labeler | Version |
| --- | --- |
`;

	const pdses = Object.entries(state?.pdses ?? {});
	const labelers = Object.entries(state?.labelers ?? {});

	// Generate the PDS table
	for (const [href, info] of pdses) {
		const host = new URL(href).host;
		const { errorAt, inviteCodeRequired, version } = info;

		const on = errorAt === undefined ? '✅' : '❌';
		const v = version || (version === null ? 'N/A' : '???');
		const invites = inviteCodeRequired === false ? 'Yes' : 'No';

		pdsTable += `| ${on} ${host} | ${invites} | ${v} |\n`;
	}

	// Generate the labeler table
	for (const [href, info] of labelers) {
		const host = new URL(href).host;
		const { errorAt, version } = info;

		const on = errorAt === undefined ? '✅' : '❌';
		const v = version || (version === null ? 'N/A' : '???');

		labelerTable += `| ${on} ${host} | ${v} |\n`;
	}

	// Read existing Markdown file, check if it's equivalent
	let shouldWrite = true;

	try {
		const source = await Bun.file(env.RESULT_FILE).text();

		if (PDS_RE.exec(source)?.[0] === pdsTable && LABELER_RE.exec(source)?.[0] === labelerTable) {
			shouldWrite = false;
		}
	} catch {}

	// Write the markdown file
	if (shouldWrite) {
		const final = template
			.replace('{{time}}', new Date().toISOString())
			.replace('{{pdsSummary}}', getPdsSummary())
			.replace('{{labelerSummary}}', getLabelerSummary())
			.replace(PDS_RE, pdsTable)
			.replace(LABELER_RE, labelerTable);

		await Bun.write(env.RESULT_FILE, final);
		console.log(`wrote to ${env.RESULT_FILE}`);
	} else {
		console.log(`writing skipped`);
	}

	function getPdsSummary() {
		let totalCount = 0;
		let onlineCount = 0;
		let offlineCount = 0;
		let blueskyHostedCount = 0;
		let nonBlueskyHostedCount = 0;

		for (const [href, info] of pdses) {
			const host = new URL(href).host;
			const { errorAt } = info;

			// `bsky.social` mainly acts as an authorization server for PDSes hosted
			// under *.host.bsky.network.
			if (host === 'bsky.social') {
				continue;
			}

			totalCount++;

			if (errorAt === undefined) {
				onlineCount++;
			} else {
				offlineCount++;
			}

			if (host.endsWith('.host.bsky.network')) {
				blueskyHostedCount++;
			} else {
				nonBlueskyHostedCount++;
			}
		}

		return (
			`**${totalCount}** instances active  \n` +
			`**${onlineCount}** online  \n` +
			`**${offlineCount}** offline  \n` +
			`**${blueskyHostedCount}** hosted by Bluesky  \n` +
			`**${nonBlueskyHostedCount}** hosted by third-parties`
		);
	}

	function getLabelerSummary() {
		let totalCount = 0;
		let onlineCount = 0;
		let offlineCount = 0;

		for (const [href, info] of labelers) {
			const { errorAt } = info;

			totalCount++;

			if (errorAt === undefined) {
				onlineCount++;
			} else {
				offlineCount++;
			}
		}

		return (
			`**${totalCount}** instances active  \n` +
			`**${onlineCount}** online  \n` +
			`**${offlineCount}** offline`
		);
	}
}
