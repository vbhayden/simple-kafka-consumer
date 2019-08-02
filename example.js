const kafkaConsumer = require("./index");

// Set everything up
kafkaConsumer.configure({
    brokers: "127.0.0.1:9092",
    saslUser: "insecure",
    saslPass: "credentials",
    consumerGroup: "example-group",
    topics: ["test-1", "test-2"]
})

// Need to call this to trigger the client's startup process.
kafkaConsumer.initConsumer(function(topic, offset, message){
    console.log(`[Kafka Consumer Example]: ${topic}@${offset}: ${message}`)
});
