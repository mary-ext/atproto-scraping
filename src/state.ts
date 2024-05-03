import * as v from '@badrap/valita';

const dateInt = v.number().chain((value) => {
	const date = new Date(value);
	const ts = date.getTime();

	if (Number.isNaN(ts)) {
		return v.err(`invalid date`);
	}

	return v.ok(ts);
});

export const serializedState = v.object({
	firehose: v.object({
		cursor: v.string().optional(),
		didWebs: v.record(
			v.object({
				errorAt: dateInt.optional(),
				pds: v.string().optional(),
				labeler: v.string().optional(),
			}),
		),
	}),
	plc: v.object({
		cursor: v.string().optional(),
	}),

	pdses: v.record(
		v.object({
			errorAt: dateInt.optional(),
			version: v.string().nullable().optional(),
		}),
	),
	labelers: v.record(
		v.object({
			errorAt: dateInt.optional(),
			version: v.string().nullable().optional(),
			did: v.string(),
		}),
	),
});

export type SerializedState = v.Infer<typeof serializedState>;

export interface DidWebInfo {
	errorAt?: number;
	pds?: string;
	labeler?: string;
}

export interface InstanceInfo {
	errorAt?: number;
	version?: string | null;
}

export interface LabelerInfo extends InstanceInfo {
	did: string;
}
