package org.aerogear.gsoc.demo.rest;

import net.wessendorf.kafka.cdi.annotation.KafkaConfig;

/**
 * Configuration for the Kafka cluster
 *
 * @author Polina Koleva
 *
 */
@KafkaConfig(bootstrapServers = "localhost:9092")
public class KafkaClusterConfig {
    
    // Global Kafka configuration for Streams
    public static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";

    // Tokens demo configuration
    public static final String TOKEN_METRICS_APPLICATION_ID = "kafka-demo-tokens";
    public static final String TOKEN_METRICS_INPUT_TOPIC = "agpush_apnsTokenDeliveryMetrics";

    // Push message demo configuration
    public static final String PUSH_METRICS_APPLICATION_ID = "kafka-demo-push";
    public static final String PUSH_METRICS_INPUT_TOPIC = "agpush_apnsTokenDeliveryMetrics";

}
