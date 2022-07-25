"use strict";
const kafka = require("kafkajs");

/**
 * @typedef {Object} SimpleKafkaConsumerConfig
 * @property {string} brokers A comma-delimited string of Kafka broker endpoints.
 * @property {boolean} useSasl Whether or not to use SASL as the auth.
 * @property {string} saslUser SASL user for the cluster.
 * @property {string} saslPass SASL password for the cluster.
 * @property {string[]} topics List of topics to consume.
 * @property {string} clientId Name of the consumer group to use for this.
 * @property {string} consumerGroup Name of the consumer group to use for this.
 */
/** Configuration settings for our client. 
 * @type {SimpleKafkaConsumerConfig}
*/
var config = undefined;

/**
 * Kafka Client to interface with our cluster.
 * @type {kafka.Kafka}
 */
var client = undefined;

/**
 * Kafka Client to interface with our cluster.
 * @type {kafka.Consumer}
 */
var consumer = undefined;

// Set up our topic objects (for the consumer) and topic history to prevent message duplication.
// Even though Kafka guarantees 
//
var topicHistory = {}

/**
 * Callback for adding two numbers.
 *
 * @callback onMessageCallback
 * @param {string} topic - Topic the message was produced to.
 * @param {number} offset - Offset of the message within that topic.
 * @param {string} message - The message itself.
 */
/**
 * Initialize the consumer with a callback function for passing the messages.
 * @param {onMessageCallback} cb Callback relaying the message and its topic.
 */
async function initConsumer(cb) {

    let saslProvided = (config.saslUser != undefined) && (config.saslPass != undefined);
    let saslExcluded = (config.useSasl === false);

    /**
     * @type {kafka.SASLOptions}
     */
    let saslConfig = {
        mechanism: "plain",
        username: config.saslUser,
        password: config.saslPass
    };
    
    for (let topic of config.topics) {
        topicHistory[topic] = {};
    }

    client = new kafka.Kafka({
        clientId: config.clientId,
        brokers: config.brokers.split(","),
        sasl: !saslProvided || saslExcluded ? undefined : saslConfig,
        logLevel: kafka.logLevel.WARN
    });

    consumer = client.consumer({

        // Group ID so that the Kafka cluster knows which messages to send based on our offset
        groupId: config.consumerGroup,

        // Whether or not to automatically commit this offset based on what we've received.  This
        // will spare us from having to commit ourselves manually.
        //
        // If this is false, then we must commit our offset manually to the Kafka cluster.  
        // Doing this might feel like a good way prevent message duplication, but that can still
        // happen.
        autoCommit: true,
        allowAutoTopicCreation: false,

        // Retrieve messages from the offset we supplied in the topic declarations
        // fromOffset: 'latest',
        readUncommitted: false
    });

    console.log("[Kafka] Subscribing to topics ...");

    await consumer.subscribe({
        topics: config.topics,
        fromBeginning: false
    });

    // These will look a bit verbose, so we'll walk through them a bit.
    //
    // The client can only be ready after having connected to the cluster, but there are
    // other connections that happen after this event.  Even though we're connected here,
    // that doesn't mean it's capable of receiving messages as the brokers themselves
    // haven't been assigned.
    //
    consumer.on("consumer.group_join", () => {

        // To clear that up, we'll assign a dummy property here to use for the "connect" events
        if (consumer.clientReady == undefined) {
            consumer.clientReady = true;
            console.log("[Kafka] Ready for brokers ...")
        } else {
            // This shouldn't ever happen?
            console.log("[Kafka] Ready again ? ...")
        }
    });

    consumer.on("consumer.rebalancing", function (message) {
        console.log("[Kafka] ConsumerGroup rebalanced ...");
    });

    // If the consumer connection crashes, then we want to restart as soon as we can.
    // We declared the reset function out of this scope to do this, so this callback and all
    // pending messages for this consumer will delayed (not lost) during the restart.
    //
    consumer.on("consumer.crash", err => {
        console.log(err);
    });

    console.log("[Kafka] Starting consumer process ...");
    
    await consumer.run({
        autoCommit: true,
        eachMessage: async (payload) => {

            let partition = payload.partition;
            let topic = payload.topic;
            let offset = Number(payload.message.offset);
            let messageStr = payload.message.value.toString();

            if (topicHistory[topic][partition] == undefined)
                topicHistory[topic][partition] = -1; 

            if (topicHistory[topic][partition] >= offset)
                return;

            cb(topic, offset, messageStr);

            topicHistory[topic][partition] = offset;
        }
    });
    
    console.log(`
    \r[Kafka]: Targeting Kafka cluster with SASL credentials:
    \r    SASL USER: ${config.saslUser},
    \r    SASL PASS: ${config.saslPass},
    \r    CLUSTER  : ${config.brokers}
    `);
}

function isReady() {
    if (consumer != undefined)
        return consumer.clientReady;

    return false;
}

async function closeConsumer() {
    if (consumer != undefined) {
        await consumer.stop();
        await consumer.disconnect();
    }
}

module.exports = {
    initConsumer,
    closeConsumer,
    isReady,

    /**
     * Configure the producer.  This is required before you can actually do anything.
     * @param {SimpleKafkaConsumerConfig} configObj 
     */
    configure: function (configObj) {
        config = configObj;
    }
}