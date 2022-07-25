const chai = require("chai");
const expect = chai.expect;
const config = require("../config");
const simpleProducer = require("simple-kafka-producer");

const simpleConsumer = require("../index");

describe("Kafka Interop", () => {

    let messages = [];
    
    function onMessage(topic, offset, message) {
        messages.push({ topic, message });
    }

    function waitFor(conditionFunc) {
        return new Promise((resolve, reject) => {
            let interval = setInterval(() => {
                let ready = conditionFunc();
                if (ready) {
                    clearInterval(interval);
                    resolve();
                }
            }, 500);
        });
    }

    before(async() => {

        simpleProducer.configure({
            brokers: config.kafka.brokers,
            saslUser: config.kafka.saslUser,
            saslPass: config.kafka.saslPass
        });
        simpleProducer.initProducer();

        simpleConsumer.configure({
            brokers: config.kafka.brokers,
            saslUser: config.kafka.saslUser,
            saslPass: config.kafka.saslPass,
            consumerGroup: "test-group",
            topics: [config.kafka.topic]
        });
        
        await simpleConsumer.initConsumer(onMessage);
    });

    beforeEach(() => {
        messages = [];
    });

    after(() => {
        simpleConsumer.closeConsumer();
    });

    it("Should consume a message", async() => {

        let topic = "test-1";
        let message = "hello!";

        simpleProducer.produceMessage(topic, message);
        
        await waitFor(() => {
            for (let msg of messages) {
                if (msg.topic == topic && msg.message == message)
                    return true;
            }

            return false;
        });
    });
})