package io.github.sahilpatkar.ffp.features;

import static io.github.sahilpatkar.ffp.testing.Trips.MINUTE;
import static io.github.sahilpatkar.ffp.testing.Trips.T0;
import static io.github.sahilpatkar.ffp.testing.Trips.trip;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.sahilpatkar.ffp.avro.TripEvent;
import io.github.sahilpatkar.ffp.testing.Trips;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class FeatureTopologyTest {

    private static final Duration ONE_SECOND = Duration.ofSeconds(1);
    private static final Duration FIVE_MINUTES = Duration.ofMinutes(5);

    private static List<FeatureRow> rowsFor(Trips.Output out, int zone, long asOfMs) {
        return out.rows.stream()
                .filter(r -> r.zoneId == zone && r.asOfMs == asOfMs)
                .collect(Collectors.toList());
    }

    @Test
    void computesEachWindowFromTheTripsInsideIt() throws Exception {
        Trips.Output out =
                Trips.run(
                        List.of(
                                trip("a", 7, 999, T0 + 30_000, 10 * MINUTE, 2.0, 10.0),
                                trip("into-7", 9, 7, T0 + 40_000, 10 * MINUTE, 1.0, 5.0),
                                trip("b", 7, 999, T0 + 6 * MINUTE, 20 * MINUTE, 3.0, 20.0)),
                        ONE_SECOND,
                        FIVE_MINUTES);

        List<FeatureRow> rows = rowsFor(out, 7, T0 + 7 * MINUTE);
        assertEquals(1, rows.size());
        FeatureRow row = rows.get(0);
        assertFalse(row.correction);

        // 5 minutes back from 00:07 holds only trip b.
        assertEquals(1L, row.w5m.tripCount);
        assertEquals(0L, row.w5m.dropoffCount);
        assertEquals(9.0, row.w5m.avgSpeedMph, 1e-9);
        assertEquals(20.0, row.w5m.avgFare, 1e-9);
        assertNull(row.w5m.pickupDropoffRatio);

        // 15 minutes back holds both trips out of zone 7 and the one into it.
        assertEquals(2L, row.w15m.tripCount);
        assertEquals(1L, row.w15m.dropoffCount);
        assertEquals(10.0, row.w15m.avgSpeedMph, 1e-9);
        assertEquals(15.0, row.w15m.avgFare, 1e-9);
        assertEquals(2.0, row.w15m.pickupDropoffRatio, 1e-9);
        assertEquals(row.w15m, row.w1h);

        assertTrue(out.tooLate.isEmpty());
    }

    @Test
    void lateEventWithinAllowedLatenessCorrectsRowsAlreadyEmitted() throws Exception {
        Trips.Output out =
                Trips.run(
                        List.of(
                                trip("on-time", 7, T0 + 10_000),
                                // Moves the watermark to just before 00:03, so the rows as of
                                // 00:01 and 00:02 are emitted.
                                trip("advance", 8, T0 + 3 * MINUTE),
                                // Belongs to the first minute, which has already been emitted.
                                trip("late", 7, T0 + 20_000)),
                        ONE_SECOND,
                        FIVE_MINUTES);

        for (long asOf : new long[] {T0 + MINUTE, T0 + 2 * MINUTE}) {
            List<FeatureRow> versions = rowsFor(out, 7, asOf);
            assertEquals(2, versions.size(), "one original and one correction as of " + asOf);
            assertFalse(versions.get(0).correction);
            assertEquals(1L, versions.get(0).w5m.tripCount);
            assertTrue(versions.get(1).correction);
            assertEquals(2L, versions.get(1).w5m.tripCount);
        }

        // Rows that were not yet emitted when the late event arrived need no correction.
        List<FeatureRow> later = rowsFor(out, 7, T0 + 3 * MINUTE);
        assertEquals(1, later.size());
        assertFalse(later.get(0).correction);
        assertEquals(2L, later.get(0).w5m.tripCount);

        assertTrue(out.tooLate.isEmpty());
    }

    @Test
    void eventBeyondAllowedLatenessIsRoutedToTheSideOutputAndNotCounted() throws Exception {
        Trips.Output out =
                Trips.run(
                        List.of(
                                trip("on-time", 7, T0 + 10_000),
                                trip("advance", 8, T0 + 10 * MINUTE),
                                trip("too-late", 7, T0 + 20_000)),
                        ONE_SECOND,
                        FIVE_MINUTES);

        // One observation per zone the trip touches.
        assertEquals(2, out.tooLate.size());
        assertTrue(out.tooLate.stream().allMatch(obs -> obs.tripId.equals("too-late")));

        assertTrue(out.rows.stream().noneMatch(r -> r.correction));
        assertEquals(1L, rowsFor(out, 7, T0 + MINUTE).get(0).w5m.tripCount);
    }

    @Test
    void quietZoneKeepsEmittingUntilItsWindowsEmptyThenEmitsOneZeroRow() throws Exception {
        Trips.Output out =
                Trips.run(List.of(trip("only", 7, T0 + 10_000)), ONE_SECOND, FIVE_MINUTES);

        List<FeatureRow> rows =
                out.rows.stream().filter(r -> r.zoneId == 7).collect(Collectors.toList());

        // One row per minute while the trip is inside the 1 hour window, then a single zero row.
        assertEquals(61, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertEquals(T0 + (i + 1) * MINUTE, rows.get(i).asOfMs);
        }
        assertEquals(1L, rows.get(4).w5m.tripCount); // as of 00:05
        assertEquals(0L, rows.get(5).w5m.tripCount); // as of 00:06: left the 5 minute window
        assertEquals(1L, rows.get(5).w15m.tripCount);
        assertEquals(1L, rows.get(59).w1h.tripCount); // as of 01:00
        assertTrue(rows.get(60).w1h.isEmpty()); // as of 01:01
    }

    @Test
    void lateEventForAZoneWithNoTimerRunningStartsItsRows() throws Exception {
        // Zone 7 has never produced a pane, so nothing is scheduled for it when its first trip
        // arrives late. It must still get corrections for the past and regular rows from then on.
        Trips.Output out =
                Trips.run(
                        List.of(trip("advance", 8, T0 + 3 * MINUTE), trip("late-first", 7, T0 + 20_000)),
                        ONE_SECOND,
                        FIVE_MINUTES);

        List<FeatureRow> rows =
                out.rows.stream().filter(r -> r.zoneId == 7).collect(Collectors.toList());
        assertEquals(61, rows.size());
        assertTrue(rows.get(0).correction); // as of 00:01, already due when the trip arrived
        assertTrue(rows.get(1).correction); // as of 00:02
        assertFalse(rows.get(2).correction); // as of 00:03, emitted on schedule
        assertEquals(T0 + 3 * MINUTE, rows.get(2).asOfMs);
        assertEquals(1L, rows.get(2).w5m.tripCount);
        assertTrue(rows.get(60).w1h.isEmpty());
    }

    /**
     * The property the whole design rests on: after corrections, the last row emitted for every
     * (zone, as-of) equals what a batch job computes from the same events. A few thousand trips
     * arrive out of order, some late enough to cause corrections and some late enough to be
     * dropped, and the streaming output is compared row by row with a direct recomputation.
     */
    @Test
    void streamingOutputMatchesBatchRecomputation() throws Exception {
        long outOfOrdernessMs = 10_000;
        long allowedLatenessMs = 5 * MINUTE;
        Random rnd = new Random(42);

        // Trips over about 100 minutes of event time, across 12 zones.
        List<TripEvent> trips = new ArrayList<>();
        Map<String, Long> arrivalTime = new HashMap<>();
        long ts = T0;
        for (int i = 0; i < 3_000; i++) {
            ts += rnd.nextInt(4_000);
            String id = "t" + i;
            trips.add(
                    trip(
                            id,
                            1 + rnd.nextInt(12),
                            1 + rnd.nextInt(12),
                            ts,
                            (2 + rnd.nextInt(40)) * MINUTE,
                            0.5 + rnd.nextDouble() * 10,
                            5 + rnd.nextDouble() * 50));
            long delay = rnd.nextInt(5_000);
            if (rnd.nextDouble() < 0.08) {
                delay += Math.min((long) (-Math.log(1 - rnd.nextDouble()) * 4 * MINUTE), 20 * MINUTE);
            }
            arrivalTime.put(id, ts + delay);
        }
        List<TripEvent> arrivals = new ArrayList<>(trips);
        arrivals.sort(Comparator.comparingLong(t -> arrivalTime.get(t.getTripId())));

        Trips.Output out =
                Trips.run(
                        arrivals,
                        Duration.ofMillis(outOfOrdernessMs),
                        Duration.ofMillis(allowedLatenessMs));

        // Which trips the pipeline must drop, by Flink's rule: a pane is closed to new events
        // once the watermark reaches its last millisecond plus the allowed lateness.
        List<TripEvent> kept = new ArrayList<>();
        Set<String> dropped = new HashSet<>();
        long watermark = Long.MIN_VALUE;
        long maxTs = Long.MIN_VALUE;
        for (TripEvent t : arrivals) {
            long paneEnd = Math.floorDiv(t.getDropoffTsMs(), MINUTE) * MINUTE + MINUTE;
            if (paneEnd - 1 + allowedLatenessMs <= watermark) {
                dropped.add(t.getTripId());
            } else {
                kept.add(t);
            }
            maxTs = Math.max(maxTs, t.getDropoffTsMs());
            watermark = maxTs - outOfOrdernessMs - 1;
        }

        assertFalse(dropped.isEmpty(), "the scenario should drop some trips");
        assertEquals(
                dropped,
                out.tooLate.stream().map(obs -> obs.tripId).collect(Collectors.toSet()));
        assertEquals(2 * dropped.size(), out.tooLate.size());
        assertTrue(out.rows.stream().anyMatch(r -> r.correction), "the scenario should cause corrections");

        // Last write wins, as it will in the online store and the feature log.
        Map<String, FeatureRow> latest = new HashMap<>();
        Map<String, Integer> originals = new HashMap<>();
        for (FeatureRow r : out.rows) {
            String key = r.zoneId + "@" + r.asOfMs;
            latest.put(key, r);
            if (!r.correction) {
                originals.merge(key, 1, Integer::sum);
            }
        }
        assertTrue(originals.values().stream().allMatch(n -> n == 1), "a row is scheduled once");

        for (FeatureRow r : latest.values()) {
            assertWindow(expected(kept, r.zoneId, r.asOfMs, FeatureWindows.W5M_MS), r.w5m, r);
            assertWindow(expected(kept, r.zoneId, r.asOfMs, FeatureWindows.W15M_MS), r.w15m, r);
            assertWindow(expected(kept, r.zoneId, r.asOfMs, FeatureWindows.W1H_MS), r.w1h, r);
        }

        // And nothing is missing: every minute at which a zone has data has a row.
        long lastAsOf = Math.floorDiv(maxTs, MINUTE) * MINUTE + MINUTE + FeatureWindows.W1H_MS;
        int checked = 0;
        for (int zone = 1; zone <= 12; zone++) {
            for (long asOf = T0 + MINUTE; asOf <= lastAsOf; asOf += MINUTE) {
                double[] e = expected(kept, zone, asOf, FeatureWindows.W1H_MS);
                if (e[0] + e[1] > 0) {
                    assertNotNull(latest.get(zone + "@" + asOf), "missing row " + zone + "@" + asOf);
                    checked++;
                }
            }
        }
        assertTrue(checked > 1_000);
    }

    /** {originCount, destinationCount, distanceSum, durationMsSum, fareSum}, computed directly. */
    private static double[] expected(List<TripEvent> kept, int zone, long asOfMs, long sizeMs) {
        double[] e = new double[5];
        for (TripEvent t : kept) {
            long paneStart = Math.floorDiv(t.getDropoffTsMs(), MINUTE) * MINUTE;
            if (paneStart < asOfMs - sizeMs || paneStart + MINUTE > asOfMs) {
                continue;
            }
            if (t.getPickupZoneId() == zone) {
                e[0]++;
                e[2] += t.getTripDistanceMiles();
                e[3] += t.getDropoffTsMs() - t.getPickupTsMs();
                e[4] += t.getFareAmount();
            }
            if (t.getDropoffZoneId() == zone) {
                e[1]++;
            }
        }
        return e;
    }

    private static void assertWindow(double[] e, WindowFeatures actual, FeatureRow row) {
        String where = " in " + row;
        assertEquals((long) e[0], actual.tripCount, "trip count" + where);
        assertEquals((long) e[1], actual.dropoffCount, "dropoff count" + where);
        assertClose(e[3] > 0 ? e[2] / (e[3] / 3_600_000.0) : null, actual.avgSpeedMph, "speed" + where);
        assertClose(e[0] > 0 ? e[4] / e[0] : null, actual.avgFare, "fare" + where);
        assertClose(e[1] > 0 ? e[0] / e[1] : null, actual.pickupDropoffRatio, "ratio" + where);
    }

    private static void assertClose(Double expected, Double actual, String what) {
        if (expected == null) {
            assertNull(actual, what);
            return;
        }
        assertNotNull(actual, what);
        // Sums are accumulated in arrival order by the pipeline and in list order here.
        assertEquals(expected, actual, 1e-9 * Math.max(1.0, Math.abs(expected)), what);
    }
}
