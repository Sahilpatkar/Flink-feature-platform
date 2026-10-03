package io.github.sahilpatkar.ffp;

import java.util.Objects;

/** Trips that ended in one event-time window, for one pickup zone. A Flink POJO. */
public class ZoneWindowCount {
    public int pickupZoneId;
    public long windowStartMs;
    public long windowEndMs;
    public long tripCount;

    public ZoneWindowCount() {}

    public ZoneWindowCount(int pickupZoneId, long windowStartMs, long windowEndMs, long tripCount) {
        this.pickupZoneId = pickupZoneId;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
        this.tripCount = tripCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ZoneWindowCount)) {
            return false;
        }
        ZoneWindowCount that = (ZoneWindowCount) o;
        return pickupZoneId == that.pickupZoneId
                && windowStartMs == that.windowStartMs
                && windowEndMs == that.windowEndMs
                && tripCount == that.tripCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pickupZoneId, windowStartMs, windowEndMs, tripCount);
    }

    @Override
    public String toString() {
        return "zone=" + pickupZoneId
                + " window=[" + windowStartMs + "," + windowEndMs + ")"
                + " trips=" + tripCount;
    }
}
