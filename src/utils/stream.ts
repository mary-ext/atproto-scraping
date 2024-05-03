// https://github.com/oven-sh/bun/issues/5648#issuecomment-1824093837
export class TextDecoderStream extends TransformStream<Uint8Array, string> {
	readonly encoding: string;
	readonly fatal: boolean;
	readonly ignoreBOM: boolean;

	constructor(label?: string, options: TextDecoderOptions = {}) {
		const decoder = new TextDecoder(label, options);

		super({
			transform(chunk: Uint8Array, controller: TransformStreamDefaultController<string>) {
				const decoded = decoder.decode(chunk);
				if (decoded.length > 0) {
					controller.enqueue(decoded);
				}
			},
			flush(controller: TransformStreamDefaultController<string>) {
				const output = decoder.decode();
				if (output.length > 0) {
					controller.enqueue(output);
				}
			},
		});

		this.encoding = decoder.encoding;
		this.fatal = decoder.fatal;
		this.ignoreBOM = decoder.ignoreBOM;
	}
}

export class LineBreakStream extends TransformStream<string, string> {
	constructor() {
		let current = '';

		super({
			transform(chunk, controller) {
				const lines = chunk.split('\n');
				const length = lines.length;

				if (length === 0) {
					// shouldn't be possible
				} else if (length === 1) {
					current = current + lines[0];
				} else if (length === 2) {
					controller.enqueue(current + lines[0]);
					current = lines[1];
				} else {
					controller.enqueue(current + lines[0]);

					for (let i = 1, il = length - 1; i < il; i++) {
						controller.enqueue(lines[i]);
					}

					current = lines[length - 1];
				}
			},
			flush(controller) {
				if (current.length > 0) {
					controller.enqueue(current);
				}
			},
		});
	}
}
