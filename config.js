let dotenv = require("dotenv").config();
if (dotenv.parsed) 
    console.log("Parsed developer env: ", dotenv.parsed);
else
    console.log("No .env found, will use container values");

module.exports = {
    
    kafka: {
        brokers: (process.env.KAFKA_BROKERS || "localhost:9092"),
        
        saslUser: (process.env.KAFKA_SASL_USER || "kafka-user"),
        saslPass: (process.env.KAFKA_SASL_PASS || "kafka-pass"),
        
        topic: (process.env.KAFKA_XAPI_TOPIC || "learner-xapi"),
    },
}

