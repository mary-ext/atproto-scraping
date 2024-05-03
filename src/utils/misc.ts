const collator = new Intl.Collator('en-US');
const reversed = new Map<string, string>();

export function getReversedDomain(url: string) {
	let cached = reversed.get(url);
	if (cached === undefined) {
		const inst = new URL(url);
		const hostname = inst.hostname;

		reversed.set(url, (cached = hostname.split('.').reverse().join('.')));
	}

	return cached;
}

export const compareString = collator.compare.bind(collator);

export function compareReversedDomain(a: string, b: string) {
	return compareString(getReversedDomain(a), getReversedDomain(b));
}
