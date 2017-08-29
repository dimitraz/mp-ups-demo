package org.aerogear.gsoc.demo.rest;

import net.wessendorf.kafka.SimpleKafkaProducer;
import net.wessendorf.kafka.cdi.annotation.Producer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.UUID;
import java.util.logging.Logger;

@Path("/produce")
@Produces(MediaType.APPLICATION_JSON)
public class MockProducer {

    Logger logger = Logger.getLogger(MockProducer.class.getName());

    private final String KAFKA_APNS_TOKEN_DELIVERY_METRICS_INPUT = "agpush_apnsTokenDeliveryMetrics";

    @Producer
    SimpleKafkaProducer<String, String> producer;

    @GET
    public Response publishMessage(String message) {

        if (producer == null) {
            return Response.status(200).entity("Producer is null").build();
        }
        else {
            String jobId = UUID.randomUUID().toString();
            producer.send(KAFKA_APNS_TOKEN_DELIVERY_METRICS_INPUT, jobId, "success");
            producer.send(KAFKA_APNS_TOKEN_DELIVERY_METRICS_INPUT, jobId, "failure");
            return Response.status(200).entity(message).build();
        }

    }

}
