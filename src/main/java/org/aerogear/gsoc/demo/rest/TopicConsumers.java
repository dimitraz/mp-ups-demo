package org.aerogear.gsoc.demo.rest;

import net.wessendorf.kafka.cdi.annotation.Consumer;

import java.util.logging.Logger;


public class TopicConsumers {

    Logger logger = Logger.getLogger(TopicConsumers.class.getName());

    @Consumer(topics = "kafka-published", groupId = "consumer-group-1")
    public void totalSuccesses(final Long total, final String jobId) {
        logger.info("Total successes per job: " + total + " jobId: " + jobId);
    }

    @Consumer(topics = "kafka-published", groupId = "consumer-group-2")
    public void totalFailures(final Long total, final String jobId) {
        logger.info("Total failures per job: " + total + " jobId: " + jobId);
    }

    @Consumer(topics = "kafka-published", groupId = "consumer-group-3")
    public void totalMessages(final Long total, final String jobId) {
        logger.info("Total messages per job: " + total + " jobId: " + jobId);
    }

}
