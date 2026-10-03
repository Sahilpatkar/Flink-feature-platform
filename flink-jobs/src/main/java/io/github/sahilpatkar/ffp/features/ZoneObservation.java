package io.github.sahilpatkar.ffp.features;

/**
 * One trip as seen from one zone. Every trip yields two: one for the zone it started in
 * ({@code origin = true}) and one for the zone it ended in. A Flink POJO.
 */
public class ZoneObservation {
    public String tripId;
    public int zoneId;
    public boolean origin;
    public double distanceMiles;
    public long durationMs;
    public double fareAmount;

    public ZoneObservation() {}

    public ZoneObservation(
            String tripId,
            int zoneId,
            boolean origin,
            double distanceMiles,
            long durationMs,
            double fareAmount) {
        this.tripId = tripId;
        this.zoneId = zoneId;
        this.origin = origin;
        this.distanceMiles = distanceMiles;
        this.durationMs = durationMs;
        this.fareAmount = fareAmount;
    }

    @Override
    public String toString() {
        return "trip=" + tripId + " zone=" + zoneId + " origin=" + origin;
    }
}
