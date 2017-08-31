package org.aerogear.gsoc.demo.rest;

import net.wessendorf.kafka.cdi.annotation.Consumer;

import java.util.logging.Logger;

public class TopicConsumers {

    Logger logger = Logger.getLogger(TopicConsumers.class.getName());

    @Consumer(topics = "successMessagesPerJob", groupId = "consumer-group-1")
    public void totalSuccesses(final String jobId, final Long total) {
        logger.info("Total successes for jobId " + jobId + ": " + total);
    }

    @Consumer(topics = "failedMessagesPerJob", groupId = "consumer-group-2")
    public void totalFailures(final String jobId, final Long total) {
        logger.info("Total failures for jobId " + jobId + ": " + total);
    }

    @Consumer(topics = "totalMessagesPerJob", groupId = "consumer-group-3")
    public void totalMessages(final String jobId, final Long total) {
        logger.info("Total messages for jobId " + jobId + ": " + total);
    }

}
