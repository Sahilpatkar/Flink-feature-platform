package io.github.sahilpatkar.ffp.features;

import static io.github.sahilpatkar.ffp.testing.Trips.MINUTE;
import static io.github.sahilpatkar.ffp.testing.Trips.T0;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class FeatureMathTest {

    private static ZonePane pane(long startMs, long origin, long destination) {
        ZonePane p = new ZonePane();
        p.zoneId = 7;
        p.paneStartMs = startMs;
        for (int i = 0; i < origin; i++) {
            p.add(new ZoneObservation("o", 7, true, 2.0, 10 * MINUTE, 10.0));
        }
        for (int i = 0; i < destination; i++) {
            p.add(new ZoneObservation("d", 7, false, 2.0, 10 * MINUTE, 10.0));
        }
        return p;
    }

    @Test
    void windowIncludesItsFirstPaneAndExcludesThePaneStartingAtAsOf() {
        long asOf = T0 + 10 * MINUTE;
        assertTrue(FeatureMath.within(pane(asOf - 5 * MINUTE, 1, 0), asOf, 5 * MINUTE));
        assertTrue(FeatureMath.within(pane(asOf - MINUTE, 1, 0), asOf, 5 * MINUTE));
        assertFalse(FeatureMath.within(pane(asOf - 6 * MINUTE, 1, 0), asOf, 5 * MINUTE));
        assertFalse(FeatureMath.within(pane(asOf, 1, 0), asOf, 5 * MINUTE));
    }

    @Test
    void eachWindowSumsOnlyItsOwnPanes() {
        long asOf = T0 + 60 * MINUTE;
        List<ZonePane> panes =
                List.of(
                        pane(asOf - 2 * MINUTE, 1, 0), // in 5m, 15m, 1h
                        pane(asOf - 10 * MINUTE, 2, 0), // in 15m, 1h
                        pane(asOf - 40 * MINUTE, 4, 0), // in 1h only
                        pane(asOf - 61 * MINUTE, 8, 0)); // in none
        FeatureRow row = FeatureMath.row(7, asOf, panes, false);
        assertEquals(1L, row.w5m.tripCount);
        assertEquals(3L, row.w15m.tripCount);
        assertEquals(7L, row.w1h.tripCount);
        assertEquals(7, row.zoneId);
        assertEquals(asOf, row.asOfMs);
    }

    @Test
    void averagesAreRatiosOfSums() {
        ZonePane p = new ZonePane();
        p.paneStartMs = T0;
        p.add(new ZoneObservation("a", 7, true, 2.0, 10 * MINUTE, 10.0)); // 12 mph alone
        p.add(new ZoneObservation("b", 7, true, 3.0, 20 * MINUTE, 20.0)); // 9 mph alone
        p.add(new ZoneObservation("c", 7, false, 9.0, 9 * MINUTE, 99.0)); // arrival: count only

        WindowFeatures f = FeatureMath.row(7, T0 + MINUTE, List.of(p), false).w5m;
        assertEquals(2L, f.tripCount);
        assertEquals(1L, f.dropoffCount);
        assertEquals(10.0, f.avgSpeedMph, 1e-9); // 5 miles in half an hour
        assertEquals(15.0, f.avgFare, 1e-9);
        assertEquals(2.0, f.pickupDropoffRatio, 1e-9);
    }

    @Test
    void averagesAreNullWhenThereIsNothingToAverage() {
        WindowFeatures none = FeatureMath.row(7, T0 + MINUTE, List.of(), false).w1h;
        assertTrue(none.isEmpty());
        assertNull(none.avgSpeedMph);
        assertNull(none.avgFare);
        assertNull(none.pickupDropoffRatio);

        WindowFeatures arrivalsOnly = FeatureMath.row(7, T0 + MINUTE, List.of(pane(T0, 0, 3)), false).w5m;
        assertFalse(arrivalsOnly.isEmpty());
        assertNull(arrivalsOnly.avgFare);
        assertEquals(0.0, arrivalsOnly.pickupDropoffRatio, 1e-9);
    }

    @Test
    void tripWithoutDurationCountsButHasNoSpeed() {
        ZonePane p = new ZonePane();
        p.paneStartMs = T0;
        p.add(new ZoneObservation("a", 7, true, 2.0, 0L, 10.0));
        WindowFeatures f = FeatureMath.row(7, T0 + MINUTE, List.of(p), false).w5m;
        assertEquals(1L, f.tripCount);
        assertNull(f.avgSpeedMph);
        assertEquals(10.0, f.avgFare, 1e-9);
    }
}
