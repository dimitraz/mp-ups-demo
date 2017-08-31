package org.aerogear.gsoc.demo.rest;

import net.wessendorf.kafka.cdi.annotation.KafkaConfig;

/**
 * Container of Kafka cluster configuration.
 * 
 * @author Polina Koleva
 *
 */
@KafkaConfig(bootstrapServers = "localhost:9092")
public class KafkaClusterConfig {
    
    // TODO add comments
    public static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";
    
    public static final String KAFKA_APPLICATION_ID = "kafka-demo-tokens";
    
    public static final String KAFKA_APNS_TOKEN_DELIVERY_METRICS_INPUT = "agpush_apnsTokenDeliveryMetrics";
    
    /**
     * Topic to which a "agpush_deliverySuccess" message will be sent if a push message was successfully send and "agpush_deliveryFailure" message otherwise.
     */
    public static final String KAFKA_PUSH_DELIVERY_METRICS_TOPIC = "agpush_pushDeliveryMetrics";
    public static final String KAFKA_METRICS_ON_DELIVERY_SUCCESS = "agpush_deliverySuccess";
    public static final String KAFKA_METRICS_ON_DELIVERY_FAILURE = "agpush_deliveryFailure";
    // TODO add comments
    public static final String KAFKA_SUCCESSFULL_MESSAGES_SENT_COUND_TOPIC = "gsoc_demo_successfullMessagesSentCount";
    public static final String KAFKA_FAILED_MESSAGES_SENT_COUNT_TOPIC = "gsoc_demo_failedMessagesSentCount";
    public static final String KAFKA_TOTAL_MESSAGES_SENT_COUNT_TOPIC = "gsoc_demo_totatMessagesSentCount";
}
