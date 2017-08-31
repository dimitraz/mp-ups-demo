package org.aerogear.gsoc.demo.rest;

import java.util.logging.Logger;

import net.wessendorf.kafka.cdi.annotation.Consumer;

/**
 * TODO add comments
 * 
 * @author Polina Koleva
 */
public class PushMessageMetricsConsumers {

    Logger logger = Logger.getLogger(PushMessageMetricsConsumers.class.getName());

    @Consumer(topics = "successMessagesPerJob", groupId = "consumer-group-push-1")
    public void totalSuccesses(final String messageId, final Long total) {
        logger.info("Total successes for id " + messageId + ": " + total);
    }

    @Consumer(topics = "failedMessagesPerJob", groupId = "consumer-group-push-2")
    public void totalFailures(final String messageId, final Long total) {
        logger.info("Total failures for id " + messageId + ": " + total);
    }

    @Consumer(topics = "totalMessagesPerJob", groupId = "consumer-group-push-3")
    public void totalMessages(final String messageId, final Long total) {
        logger.info("Total messages for id " + messageId + ": " + total);
    }

}
