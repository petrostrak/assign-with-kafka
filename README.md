## Assign with Kafka
A simple Kafka system

A kafka producer that produces 1000 mesasges into a Kafka topic as a batch. The producer produces messages that have different states, some states are failed, some are completed and some are in progress.

The Kafka consumer will
* Read from this topic and parses these states
* Stores them into a persistent store.

The persistent store now will have various messages that are either successful, in progress or failed.

A test suit will be able to correctly assert that there was no messages dropped and the messages were correctly parsed into each bucket of successful, in progress or failed.

The system is not static but is a streaming system.
