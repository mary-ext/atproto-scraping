import * as v from '@badrap/valita';

export type DidDocument = v.Infer<typeof didDocument>;

const verificationMethod = v.object({
	id: v.string(),
	type: v.string(),
	controller: v.string(),
	publicKeyMultibase: v.string().optional(),
});

const service = v.object({
	id: v.string(),
	type: v.string(),
	serviceEndpoint: v.union(v.string(), v.record(v.unknown())),
});

export const didDocument = v.object({
	id: v.string(),
	alsoKnownAs: v.array(v.string()).optional(),
	verificationMethod: v.array(verificationMethod).optional(),
	service: v.array(service).optional(),
});
