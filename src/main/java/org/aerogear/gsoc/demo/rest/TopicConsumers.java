package org.aerogear.gsoc.demo.rest;

import com.google.gson.Gson;
import net.wessendorf.kafka.cdi.annotation.Consumer;

import javax.json.JsonArray;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


public class TopicConsumers {

    Logger logger = Logger.getLogger(TopicConsumers.class.getName());

    private Map<String, JobMetrics> metrics = new HashMap<>();

    @Consumer(topics = "successMessagesPerJob", groupId = "consumer-group-1")
    public void totalSuccesses(final String jobId, final Long total) {
        logger.info("Total successes for jobId " + jobId + ": " + total);
        updateSuccesses(jobId, total);
    }

    @Consumer(topics = "failedMessagesPerJob", groupId = "consumer-group-2")
    public void totalFailures(final String jobId, final Long total) {
        logger.info("Total failures for jobId " + jobId + ": " + total);
        updateFailures(jobId, total);
    }

    @Consumer(topics = "totalMessagesPerJob", groupId = "consumer-group-3")
    public void totalMessages(final String jobId, final Long total) {
        logger.info("Total messages for jobId " + jobId + ": " + total);
        updateTotal(jobId, total);
    }

    private void updateSuccesses(String jobId, Long successes) {
        if(!metrics.containsKey(jobId)) {
            metrics.put(jobId, new JobMetrics(jobId, successes, null, null));
        } else {
            metrics.get(jobId).setSuccesses(successes);
        }
    }

    private void updateFailures(String jobId, Long failures) {
        if(!metrics.containsKey(jobId)) {
            metrics.put(jobId, new JobMetrics(jobId, null, failures, null));
        } else {
            metrics.get(jobId).setFailures(failures);
        }
    }

    private void updateTotal(String jobId, Long total) {
        if(!metrics.containsKey(jobId)) {
            metrics.put(jobId, new JobMetrics(jobId, null, null, total));
        } else {
            metrics.get(jobId).setTotal(total);
        }
    }

    private class JobMetrics {
        private String jobId;
        private Long successes;
        private Long failures;
        private Long total;

        public JobMetrics(final String jobId, Long successes, Long failures, Long total) {
            this.jobId = jobId;
            this.successes = successes;
            this.failures = failures;
            this.total = total;
        }

        public void setSuccesses(Long successes) {
            if(successes >= 0) {
                this.successes = successes;
            }
        }

        public void setFailures(Long failures) {
            if(successes >= 0) {
                this.failures = failures;
            }
        }

        public void setTotal(Long total) {
            if(total >= 0) {
                this.total = total;
            }
        }
    }

}
