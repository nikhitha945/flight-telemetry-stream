package com.streaming.telemetry;

/**
 * One telemetry reading from one aircraft at one moment in time.
 * This is the JSON message that gets published to the Kafka topic.
 */
public record TelemetryEvent(
        String aircraftId,
        long eventTime,
        double latitude,
        double longitude,
        double altitudeFt,
        double speedKts,
        double batteryPct,
        double motorTempC) {
}
