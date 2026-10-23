package com.streaming.telemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Every second, ticks every simulated aircraft and publishes its
 * telemetry event to Kafka. The aircraft ID is the message KEY -
 * that is what guarantees all events for one aircraft land in the
 * same partition, in order.
 *
 * The event object is serialized to JSON by Spring Kafka's
 * JsonSerializer (configured in application.yml), so there is no
 * manual Jackson code here - that keeps this class untouched by
 * the Jackson 2 -> 3 move in Spring Boot 4.
 */
@Component
public class TelemetryProducer {

    public static final String TOPIC = "telemetry.raw";

    private static final Logger log = LoggerFactory.getLogger(TelemetryProducer.class);

    private final KafkaTemplate<String, TelemetryEvent> kafkaTemplate;

    // Five aircraft starting around San Jose, CA
    private final List<Aircraft> fleet = List.of(
            new Aircraft("AC-101", 37.3382, -121.8863, 30, 85, 1200, 96, 68),
            new Aircraft("AC-102", 37.3482, -121.8763, 70, 93, 1500, 95, 70),
            new Aircraft("AC-103", 37.3282, -121.8963, 110, 101, 1800, 94, 72),
            new Aircraft("AC-104", 37.3582, -121.8663, 150, 109, 2100, 93, 74),
            new Aircraft("AC-105", 37.3182, -121.9063, 190, 117, 2400, 92, 76)
    );

    public TelemetryProducer(KafkaTemplate<String, TelemetryEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRate = 1000)
    public void publish() {
        for (Aircraft aircraft : fleet) {
            aircraft.tick();
            TelemetryEvent event = aircraft.toEvent();
            kafkaTemplate.send(TOPIC, event.aircraftId(), event);
            log.info("sent {}", event);
        }
    }
}
