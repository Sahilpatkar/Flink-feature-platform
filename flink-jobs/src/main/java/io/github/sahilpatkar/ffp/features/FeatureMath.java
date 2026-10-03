package io.github.sahilpatkar.ffp.features;

/**
 * Turns panes into features. Pure functions with no Flink types, so the streaming job, the
 * tests, and (in week 4) the batch backfill all compute features with exactly this code.
 */
public final class FeatureMath {
    private static final double MS_PER_HOUR = 3_600_000.0;

    private FeatureMath() {}

    public static FeatureRow row(int zoneId, long asOfMs, Iterable<ZonePane> panes, boolean correction) {
        ZonePane s5m = new ZonePane();
        ZonePane s15m = new ZonePane();
        ZonePane s1h = new ZonePane();
        for (ZonePane pane : panes) {
            if (within(pane, asOfMs, FeatureWindows.W5M_MS)) {
                s5m.add(pane);
            }
            if (within(pane, asOfMs, FeatureWindows.W15M_MS)) {
                s15m.add(pane);
            }
            if (within(pane, asOfMs, FeatureWindows.W1H_MS)) {
                s1h.add(pane);
            }
        }
        FeatureRow row = new FeatureRow();
        row.zoneId = zoneId;
        row.asOfMs = asOfMs;
        row.w5m = features(s5m);
        row.w15m = features(s15m);
        row.w1h = features(s1h);
        row.correction = correction;
        return row;
    }

    /** A pane belongs to the window [asOf - size, asOf) when it lies entirely inside it. */
    static boolean within(ZonePane pane, long asOfMs, long sizeMs) {
        return pane.paneStartMs >= asOfMs - sizeMs
                && pane.paneStartMs + FeatureWindows.PANE_MS <= asOfMs;
    }

    static WindowFeatures features(ZonePane sum) {
        WindowFeatures f = new WindowFeatures();
        f.tripCount = sum.originCount;
        f.dropoffCount = sum.destinationCount;
        f.avgSpeedMph =
                sum.durationMsSum > 0
                        ? sum.distanceMilesSum / (sum.durationMsSum / MS_PER_HOUR)
                        : null;
        f.avgFare = sum.originCount > 0 ? sum.fareSum / sum.originCount : null;
        f.pickupDropoffRatio =
                sum.destinationCount > 0
                        ? (double) sum.originCount / sum.destinationCount
                        : null;
        return f;
    }
}
