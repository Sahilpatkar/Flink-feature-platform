package io.github.sahilpatkar.ffp.features;

import java.util.Objects;

/**
 * The features of one zone as of {@code asOfMs}, covering event times strictly before it. The
 * pair (zoneId, asOfMs) is the row's identity: a later row with the same pair replaces it.
 */
public class FeatureRow {
    public int zoneId;
    public long asOfMs;
    public WindowFeatures w5m;
    public WindowFeatures w15m;
    public WindowFeatures w1h;
    /** True when this row replaces one already emitted, because late data changed it. */
    public boolean correction;

    public FeatureRow() {}

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FeatureRow)) {
            return false;
        }
        FeatureRow that = (FeatureRow) o;
        return zoneId == that.zoneId
                && asOfMs == that.asOfMs
                && correction == that.correction
                && Objects.equals(w5m, that.w5m)
                && Objects.equals(w15m, that.w15m)
                && Objects.equals(w1h, that.w1h);
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoneId, asOfMs, correction, w5m, w15m, w1h);
    }

    @Override
    public String toString() {
        return "zone=" + zoneId + " as_of=" + asOfMs + " correction=" + correction
                + " 5m=" + w5m + " 15m=" + w15m + " 1h=" + w1h;
    }
}
