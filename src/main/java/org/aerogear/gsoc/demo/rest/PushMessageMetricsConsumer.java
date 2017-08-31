package org.aerogear.gsoc.demo.rest;

import java.util.logging.Logger;

import net.wessendorf.kafka.cdi.annotation.Consumer;

/**
 * TODO add comments
 * 
 * @author Polina Koleva
 */
public class PushMessageMetricsConsumer {

    Logger logger = Logger.getLogger(PushMessageMetricsConsumer.class.getName());

    @Consumer(topics = KafkaClusterConfig.KAFKA_SUCCESSFULL_MESSAGES_SENT_COUND_TOPIC, groupId = "consumer-group-1")
    public void totalSuccesses(final String messageId, final Long total) {
        logger.info("Total successfully sent message for id " + messageId + ": " + total);
    }

    @Consumer(topics = KafkaClusterConfig.KAFKA_FAILED_MESSAGES_SENT_COUNT_TOPIC, groupId = "consumer-group-2")
    public void totalFailures(final String messageId, final Long total) {
        logger.info("Total failed messages for id " + messageId + ": " + total);
    }

    @Consumer(topics = KafkaClusterConfig.KAFKA_TOTAL_MESSAGES_SENT_COUNT_TOPIC, groupId = "consumer-group-3")
    public void totalMessages(final String messageId, final Long total) {
        logger.info("Total messages for id " + messageId + ": " + total);
    }

    
}
