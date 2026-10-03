package io.github.sahilpatkar.ffp.features;

/**
 * The time grid every feature is defined on. Events are first aggregated into one-minute panes;
 * every feature window is a whole number of panes, and one row per zone is produced at every pane
 * boundary.
 */
public final class FeatureWindows {
    public static final long PANE_MS = 60_000L;
    public static final long W5M_MS = 5 * PANE_MS;
    public static final long W15M_MS = 15 * PANE_MS;
    public static final long W1H_MS = 60 * PANE_MS;
    /** The longest window; panes older than this (plus allowed lateness) can be forgotten. */
    public static final long MAX_WINDOW_MS = W1H_MS;

    private FeatureWindows() {}
}
