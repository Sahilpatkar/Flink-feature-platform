package io.github.sahilpatkar.ffp.features;

import java.util.Objects;

/** The features of one zone over one window. Averages are null when there is nothing to average. */
public class WindowFeatures {
    public long tripCount;
    public long dropoffCount;
    /** Total distance over total time, which is robust to very short trips. */
    public Double avgSpeedMph;
    public Double avgFare;
    /** Trips leaving the zone per trip arriving; null when none arrived. */
    public Double pickupDropoffRatio;

    public WindowFeatures() {}

    public boolean isEmpty() {
        return tripCount == 0 && dropoffCount == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WindowFeatures)) {
            return false;
        }
        WindowFeatures that = (WindowFeatures) o;
        return tripCount == that.tripCount
                && dropoffCount == that.dropoffCount
                && Objects.equals(avgSpeedMph, that.avgSpeedMph)
                && Objects.equals(avgFare, that.avgFare)
                && Objects.equals(pickupDropoffRatio, that.pickupDropoffRatio);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tripCount, dropoffCount, avgSpeedMph, avgFare, pickupDropoffRatio);
    }

    @Override
    public String toString() {
        return "{trips=" + tripCount + " dropoffs=" + dropoffCount
                + " speed=" + avgSpeedMph + " fare=" + avgFare
                + " ratio=" + pickupDropoffRatio + "}";
    }
}
