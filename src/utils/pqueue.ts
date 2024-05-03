export class PromiseQueue {
	#queue: { deferred: PromiseWithResolvers<any>; fn: () => any }[] = [];

	#max: number;
	#current = 0;

	constructor({ max = 4 }: { max?: number } = {}) {
		this.#max = max;
	}

	add<T>(fn: () => Promise<T>): Promise<T> {
		const deferred = Promise.withResolvers<T>();

		this.#queue.push({ deferred, fn });
		this.#run();

		return deferred.promise;
	}

	#run() {
		if (this.#queue.length > 0 && this.#current <= this.#max) {
			const { deferred, fn } = this.#queue.shift()!;
			this.#current++;

			const promise = new Promise((r) => r(fn()));

			const done = () => {
				this.#current--;
				this.#run();
			};

			promise.then(
				(res) => {
					done();
					deferred.resolve(res);
				},
				(err) => {
					done();
					deferred.reject(err);
				},
			);
		}
	}
}
