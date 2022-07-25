const config = require("./config");
const simpleConsumer = require("./index");

async function main() {
    
    simpleConsumer.configure({
        brokers: config.kafka.brokers,
        saslUser: config.kafka.saslUser,
        saslPass: config.kafka.saslPass,
        consumerGroup: "test-group",
        topics: [config.kafka.topic]
    });
    
    await simpleConsumer.initConsumer(function(topic, offset, message){
        console.log(`[Kafka Consumer Example]: ${topic}@${offset}: ${message}`);
    });
}

main();