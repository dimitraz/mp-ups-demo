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
public class PushMessageMetricsStream {

    Logger logger = Logger.getLogger(PushMessageMetricsStream.class.getName());

    private KafkaStreams streams;

    //TODO add comments
    private void startup(@Observes @Initialized(ApplicationScoped.class) Object init) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaClusterConfig.KAFKA_APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaClusterConfig.KAFKA_BOOTSTRAP_SERVER);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final KStreamBuilder builder = new KStreamBuilder();

        // Read from the source stream
        final KStream<String, String> source = builder.stream(KafkaClusterConfig.KAFKA_PUSH_DELIVERY_METRICS_TOPIC);

        // Count messages that were successfully sent per message id
        final KTable<String, Long> successfullMessageCountsPerMessageId = source.filter((key, value) -> value.equals(KafkaClusterConfig.KAFKA_METRICS_ON_DELIVERY_SUCCESS))
                // message id
                .groupByKey()
                .count("successfullMessagesCountPerMessageId");

        // write to an output topic <messageId, #successfullySentMessages>
        successfullMessageCountsPerMessageId.to(Serdes.String(), Serdes.Long(), KafkaClusterConfig.KAFKA_SUCCESSFULL_MESSAGES_SENT_COUND_TOPIC);

        // Count messages that were successfully sent per message id
        final KTable<String, Long> failedMessageCountsPerMessageId = source.filter((key, value) -> value.equals(KafkaClusterConfig.KAFKA_METRICS_ON_DELIVERY_FAILURE))
                .groupByKey()
                .count("failedMessagesCountPerMessageId");
        
        // write to an output topic <messageId, #failedMessages>
        failedMessageCountsPerMessageId.to(Serdes.String(), Serdes.Long(), KafkaClusterConfig.KAFKA_FAILED_MESSAGES_SENT_COUNT_TOPIC);


        // Count total messages per message id
        source.groupByKey()
                .count("totalMessagesCountPerMessageId")
                // write to an output topic <messageId, #totalMessagePerId>
                .to(Serdes.String(), Serdes.Long(), KafkaClusterConfig.KAFKA_TOTAL_MESSAGES_SENT_COUNT_TOPIC);


        streams = new KafkaStreams(builder, props);
        streams.start();
    }

    @PreDestroy
    private void shutdown() {
        logger.warning("Shutting down the streams.");
        streams.close();
    }
}
