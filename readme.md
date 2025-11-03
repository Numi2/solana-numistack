low latency solana data and execution stack

geyser-plugin-ultra: in-process validator plugin that emits account, transaction, block, and slot updates.

ultra-aggregator: local daemon that accepts the stream over a Unix domain socket and fans out to consumers.

ys-consumer: client for a remote Yellowstone gRPC endpoint if you do not run a validator.

jito-client / jito-bundle: gRPC client to build and submit signed transaction bundles to the Jito Block Engine.


---

goals for the design: 

	•	No blocking in validator hot paths. The plugin only enqueues and exits.
	•	Single UDS hop to aggregator. Low latency and low jitter.
	•	Clear backpressure: bounded queues, batch writes, counters.
	•	Works both with your own validator or any Yellowstone provider.