const kafka = require("kafka-node");

/**
 * @typedef {Object} SimpleKafkaConsumerConfig
 * @property {string} brokers A comma-delimited string of Kafka broker endpoints.
 * @property {boolean} useSasl Whether or not to use SASL as the auth.
 * @property {string} saslUser SASL user for the cluster.
 * @property {string} saslPass SASL password for the cluster.
 * @property {string[]} topics List of topics to consume.
 * @property {string} consumerGroup Name of the consumer group to use for this.
 */
/** Configuration settings for our client. 
 * @type {SimpleKafkaConsumerConfig}
*/
var config = undefined;

/**
 * Kafka Client to interface with our cluster.
 * @type {kafka.ConsumerGroup}
 */
var group = undefined;

// Set up our topic objects (for the consumer) and topic history to prevent message duplication.
// Even though Kafka guarantees 
//
var topicHistory = {}

// Reset our consumer if anything should go wrong, this will clear out the existing consumer,
// including its callbacks, and recreate one in its place.
//
function resetConsumer(cb) {

    console.log("RESET TRIGGERED ...");

    if (group != undefined) {
        group.close(() => { initConsumer(cb) });
    } else {
        initConsumer(cb);
    }
}

// Broadcast that something happened.  This will give us a common failure message for either the try/catch
// block or the Consumer itself.
//
function onFailure(error, cb) {

    console.log(error);
    resetConsumer(cb);
}

function log(message) {
    return () => { console.log(message) }
}

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
function initConsumer(cb) {

    group = new kafka.ConsumerGroup({
    
        // Group ID so that the Kafka cluster knows which messages to send based on our offset
        groupId: config.consumerGroup,
    
        // Whether or not to automatically commit this offset based on what we've received.  This
        // will spare us from having to commit ourselves manually.
        //
        // If this is false, then we must commit our offset manually to the Kafka cluster.  
        // Doing this might feel like a good way prevent message duplication, but that can still
        // happen.
        autoCommit: true,
    
        // Retrieve messages from the offset we supplied in the topic declarations
        fromOffset: 'latest',
    
        // Where to find our brokers
        kafkaHost: config.brokers,
    
        // SASL credentials and mechanic
        sasl: (config.useSasl !== true ? undefined : {
            mechanism: "plain",
            username: config.saslUser,
            password: config.saslPass
        }),
    }, config.topics);

    // These will look a bit verbose, so we'll walk through them a bit.
    //
    // The client can only be ready after having connected to the cluster, but there are
    // other connections that happen after this event.  Even though we're connected here,
    // that doesn't mean it's capable of receiving messages as the brokers themselves
    // haven't been assigned.
    //
    group.client.on("ready", () => {
        
        // To clear that up, we'll assign a dummy property here to use for the "connect" events
        if (group.clientReady == undefined) {
            group.clientReady = true;
            console.log("[Kafka] Ready for brokers ...")
        } else {
            // This shouldn't ever happen?
            console.log("[Kafka] Ready again ? ...")
        }
    });

    // This is fired for 3 different things, none of which are actually documented and the 
    // wrapper itself doesn't provide any sort of callback argument to clear this up.
    //
    group.client.on("connect", msg => {

        // Dummy property to check if this is our first time connecting to the cluster
        if (group.clusterFound == undefined) {
            group.clusterFound = true;
            console.log("[Kafka] Connected to cluster ...")
        } 
        
        // Check if we got that dummy property from the "ready" event, meaning we're now looking
        // to get actual broker assignments.
        else if (group.clientReady) {
            
            if (!group.brokersFound) {
                group.brokersFound = true;
                console.log(`[Kafka] Established broker connection ...`) 
            } 
        }
    })

    // The rest of these might have similar nuance to the "connect" event, but I don't feel like looking
    // through them for the sake of verbose logs.
    //
    group.client.on("reconnect", log("[Kafka] ConsumerGroup client reconnected!"))
    group.client.on("socket_error", log("[Kafka] ConsumerGroup socket error!"))
    group.client.on("zkReconnect", log("[Kafka] ConsumerGroup ZooKeeper reconnected!"))
    group.client.on("brokersChanged",log("[Kafka] ConsumerGroup client brokers have changed!"))
    group.client.on("error", log("[Kafka] ConsumerGroup client error!"))

    console.log(`
    \r[Kafka]: Targeting Kafka cluster with SASL credentials:
    \r    SASL USER: ${config.saslUser},
    \r    SASL PASS: ${config.saslPass},
    \r    CLUSTER  : ${config.brokers}
    \r    GROUP GEN: ${group.generationId}
    \r    GROUP MID: ${group.memberId}
    `);

    group.on("rebalancing", function(message) {
        console.log("[Kafka] ConsumerGroup rebalanced ...");
    });

    // Whenever a new message comes in, broadcast that to every open WebSocket,
    // these messages will include the topic, offset, and the message itself
    //
    group.on('message', function (message) {

        if (topicHistory[message.topic] == undefined)
            topicHistory[message.topic] = {}

        // Check if we already received this
        let lastOffset = topicHistory[message.topic][message.partition];
        if (lastOffset != undefined && lastOffset >= message.offset) {
            return;
        }

        // If this is a new offset, broadcast it
        topicHistory[message.topic][message.partition] = message.offset;
        
        cb(message.topic, message.offset, message.value)
    });

    // If the consumer connection crashes, then we want to restart as soon as we can.
    // We declared the reset function out of this scope to do this, so this callback and all
    // pending messages for this consumer will delayed (not lost) during the restart.
    //
    group.on("error", err => {
        onFailure(err, cb)
    });
}

module.exports = {
    initConsumer,

    /**
     * Configure the producer.  This is required before you can actually do anything.
     * @param {SimpleKafkaConsumerConfig} configObj 
     */
    configure: function(configObj) {
        config = configObj;
    }
}