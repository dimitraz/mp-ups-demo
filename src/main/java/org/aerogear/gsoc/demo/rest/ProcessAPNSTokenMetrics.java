package org.aerogear.gsoc.demo.rest;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * TODO add comments
 */
@ApplicationScoped
public class ProcessAPNSTokenMetrics {

    Logger logger = Logger.getLogger(ProcessAPNSTokenMetrics.class.getName());

    private KafkaStreams streams;

    private void startup(@Observes @Initialized(ApplicationScoped.class) Object init) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaClusterConfig.TOKEN_METRICS_APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaClusterConfig.KAFKA_BOOTSTRAP_SERVER);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final KStreamBuilder builder = new KStreamBuilder();

        // Read from the source stream
        final KStream<String, String> source = builder.stream(KafkaClusterConfig.TOKEN_METRICS_INPUT_TOPIC);

        // Count successes per job
        final KTable<String, Long> successCountsPerJob = source.filter((key, value) -> value.equals("success"))
                .groupByKey()
                .count("successTokensPerJob");

        successCountsPerJob.to(Serdes.String(), Serdes.Long(), "successTokensPerJob");


        // Count failures per job
        final KTable<String, Long> failCountsPerJob = source.filter((key, value) -> value.equals("failure"))
                .groupByKey()
                .count("failedTokensPerJob");

        failCountsPerJob.to(Serdes.String(), Serdes.Long(), "failedTokensPerJob");


        // Count total messages per job
        source.groupByKey()
                .count("totalTokensPerJob")
                .to(Serdes.String(), Serdes.Long(), "totalTokensPerJob");


        streams = new KafkaStreams(builder, props);
        streams.start();
    }

    @PreDestroy
    private void shutdown() {
        logger.warning("Shutting down the streams.");
        streams.close();
    }

}
