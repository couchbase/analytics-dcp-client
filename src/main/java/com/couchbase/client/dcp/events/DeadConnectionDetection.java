/*
 * Copyright (c) 2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.couchbase.client.dcp.conductor.Conductor;

public class DeadConnectionDetection {

    private static final Logger LOGGER = Logger.getLogger(DeadConnectionDetection.class.getName());
    private final Conductor conductor;
    private final long interval;
    private long lastRun;

    public DeadConnectionDetection(Conductor conductor) {
        this.conductor = conductor;
        this.interval = conductor.getEnv().getDeadConnectionDetectionInterval();
        lastRun = System.currentTimeMillis();
    }

    public void run() {
        long now = System.currentTimeMillis();
        if (now - lastRun > interval) {
            lastRun = now;
            LOGGER.log(Level.WARNING, "Running Dead connection detection");
            conductor.reviveDeadConnections();
        }
    }

    public long timeToCheck() {
        long now = System.currentTimeMillis();
        long diff = now - lastRun;
        return Math.max(0, interval - diff);
    }
}
