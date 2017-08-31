package org.aerogear.gsoc.demo.rest;

import java.util.Properties;
import java.util.logging.Logger;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

/**
 * TODO add comments
 * 
 * @author Polina Koleva
 */
@ApplicationScoped
public class ProcessPushMessageMetrics {

    Logger logger = Logger.getLogger(ProcessPushMessageMetrics.class.getName());

    private KafkaStreams streams;

    private void startup(@Observes @Initialized(ApplicationScoped.class) Object init) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaClusterConfig.PUSH_METRICS_APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaClusterConfig.KAFKA_BOOTSTRAP_SERVER);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final KStreamBuilder builder = new KStreamBuilder();

        // Read from the source stream
        final KStream<String, String> source = builder.stream(KafkaClusterConfig.PUSH_METRICS_INPUT_TOPIC);

        // Count successes per message id
        final KTable<String, Long> successCountsPerJob = source.filter((key, value) -> value.equals("success"))
                .groupByKey()
                .count("successMessagesPerJob");

        successCountsPerJob.to(Serdes.String(), Serdes.Long(), "successMessagesPerJob");


        // Count messages that were successfully sent per message id
        final KTable<String, Long> failCountsPerJob = source.filter((key, value) -> value.equals("failure"))
                .groupByKey()
                .count("failedMessagesPerJob");

        failCountsPerJob.to(Serdes.String(), Serdes.Long(), "failedMessagesPerJob");

        // Count total messages per message id
        source.groupByKey()
                .count("totalMessagesPerJob")
                .to(Serdes.String(), Serdes.Long(), "totalMessagesPerJob");


        streams = new KafkaStreams(builder, props);
        streams.start();
    }

    @PreDestroy
    private void shutdown() {
        logger.warning("Shutting down the streams.");
        streams.close();
    }

}
