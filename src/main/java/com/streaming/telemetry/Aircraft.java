package com.streaming.telemetry;

import java.util.Random;

/**
 * A simulated aircraft with mutable flight state.
 * Each tick() advances the aircraft by one second of flight.
 */
public class Aircraft {

    private final String id;
    private final Random random = new Random();

    private double latitude;
    private double longitude;
    private double headingDeg;
    private double speedKts;
    private double altitudeFt;
    private double batteryPct;
    private double motorTempC;

    public Aircraft(String id, double latitude, double longitude, double headingDeg,
                    double speedKts, double altitudeFt, double batteryPct, double motorTempC) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.headingDeg = headingDeg;
        this.speedKts = speedKts;
        this.altitudeFt = altitudeFt;
        this.batteryPct = batteryPct;
        this.motorTempC = motorTempC;
    }

    /** Advance the simulation by one second. */
    public void tick() {
        // Distance covered in one second (knots -> metres/second)
        double distMeters = speedKts * 0.514444;
        double headingRad = Math.toRadians(headingDeg);

        latitude += (distMeters * Math.cos(headingRad)) / 111_320.0;
        longitude += (distMeters * Math.sin(headingRad))
                / (111_320.0 * Math.cos(Math.toRadians(latitude)));

        // Gentle heading drift so the flight path curves naturally
        headingDeg += (random.nextDouble() - 0.5) * 4.0;

        // Battery drains slowly (exaggerated a little so it's visible in a demo)
        batteryPct = Math.max(0, batteryPct - (0.02 + random.nextDouble() * 0.03));

        // Motor temperature wobbles around a warm operating range
        motorTempC += (random.nextDouble() - 0.45) * 1.5;
        motorTempC = Math.max(55.0, Math.min(98.0, motorTempC));

        // Slight altitude wander
        altitudeFt += (random.nextDouble() - 0.5) * 20.0;
        altitudeFt = Math.max(800.0, Math.min(3_500.0, altitudeFt));
    }

    public TelemetryEvent toEvent() {
        return new TelemetryEvent(
                id,
                System.currentTimeMillis(),
                round(latitude), round(longitude),
                round(altitudeFt), round(speedKts),
                round(batteryPct), round(motorTempC));
    }

    private double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    public String getId() {
        return id;
    }
}
