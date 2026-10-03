package io.github.sahilpatkar.ffp.features;

/**
 * Everything the features need to know about one zone in one minute. Only sums and counts, so
 * panes add up to any window and a late event is a cheap in-place update. A Flink POJO.
 */
public class ZonePane {
    public int zoneId;
    public long paneStartMs;
    /** Trips that started in this zone (and ended in this minute). */
    public long originCount;
    /** Trips that ended in this zone. */
    public long destinationCount;
    // The next three cover origin trips only.
    public double distanceMilesSum;
    public long durationMsSum;
    public double fareSum;

    public ZonePane() {}

    public void add(ZoneObservation obs) {
        if (!obs.origin) {
            destinationCount++;
            return;
        }
        originCount++;
        fareSum += obs.fareAmount;
        // A trip with no duration has no speed; it still counts as a trip.
        if (obs.durationMs > 0 && obs.distanceMiles >= 0) {
            distanceMilesSum += obs.distanceMiles;
            durationMsSum += obs.durationMs;
        }
    }

    public void add(ZonePane other) {
        originCount += other.originCount;
        destinationCount += other.destinationCount;
        distanceMilesSum += other.distanceMilesSum;
        durationMsSum += other.durationMsSum;
        fareSum += other.fareSum;
    }

    @Override
    public String toString() {
        return "zone=" + zoneId + " pane=" + paneStartMs
                + " origin=" + originCount + " destination=" + destinationCount;
    }
}
