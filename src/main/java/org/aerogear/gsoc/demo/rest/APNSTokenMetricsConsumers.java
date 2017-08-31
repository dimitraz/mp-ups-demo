package org.aerogear.gsoc.demo.rest;

import net.wessendorf.kafka.cdi.annotation.Consumer;

import java.util.logging.Logger;

/**
 * Consume total amount of tokens, successes and failures
 * and log the result to console
 */
public class APNSTokenMetricsConsumers {

    Logger logger = Logger.getLogger(APNSTokenMetricsConsumers.class.getName());

    @Consumer(topics = "successTokensPerJob", groupId = "consumer-group-1")
    public void totalSuccesses(final String jobId, final Long total) {
        logger.info("Total successes for jobId " + jobId + ": " + total);
    }

    @Consumer(topics = "failedTokensPerJob", groupId = "consumer-group-2")
    public void totalFailures(final String jobId, final Long total) {
        logger.info("Total failures for jobId " + jobId + ": " + total);
    }

    @Consumer(topics = "totalTokensPerJob", groupId = "consumer-group-3")
    public void totalMessages(final String jobId, final Long total) {
        logger.info("Total messages for jobId " + jobId + ": " + total);
    }

}
