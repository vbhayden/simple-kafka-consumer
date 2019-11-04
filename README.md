## Simplified Kafka Consumer
This is a Node module to streamline Kafka integration for Node services.  It's intended to provide a common and heavily-abstracted Kafka client to use for consuming messages from the Kafka cluster.

This is essentially a wrapper for the **[kafka-node](https://www.npmjs.com/package/kafka-node)** library.

### Installation
You can install this through NPM:
```
npm install simple-kafka-consumer --save
```

### Setup
Setup for a consumer is a bit more involved than a producer, but overall not bad.  This adapter supports plaintext SASL as an authorization mechanism, so you'll need to provide those credentials if that's how your cluster is configured.

```
const kafkaConsumer = require("simple-kafka-consumer");

// Set everything up
kafkaConsumer.configure({
    brokers: "127.0.0.1:9092",
    saslUser: "insecure",
    saslPass: "credentials",
    consumerGroup: "example-group",
    topics: ["test-1", "test-2"]
})
```

Once you've assigned your Kafka information, you'll need to initialize the client before you can send anything.

```
kafkaProducer.initProducer();
```

After that, you should be able to send messages to a given Kafka topic with:

```
kafkaConsumer.initConsumer(function(topic, offset, message){
    console.log(`[Kafka Consumer Example]: ${topic}@${offset}: ${message}`)
});
```

That's all there is to it.

### Topic Offset Initialization
At the moment, this will attempt to read from wherever the consumer group left off.  However, new groups will start at the most recent topic commit -- not read the entire stream.  That will be added later.

### Failure Tolerance
This adapter is set up to cache any messages that failed to send and try to resend them after a short delay.  You shouldn't need to handle any of this yourself.  Do note:
- If the connection to your Kafka cluster is lost, then the adapter will begin caching all messages and will attemtp to send them / flush its cache in reconnection.  
- If for some reason the service is killed during this process, **those cached messages will be lost**.
