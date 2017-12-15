/*
 * Copyright (c) 2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.dcp.conductor.Conductor;

public class DeadConnectionDetection {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int TIMEOUT = 2000;
    private static final int ATTEMPTS = 1;

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
            LOGGER.warn("Running Dead connection detection");
            conductor.reviveDeadConnections(TIMEOUT, ATTEMPTS);
        }
    }

    public long timeToCheck() {
        long now = System.currentTimeMillis();
        long diff = now - lastRun;
        return Math.max(0, interval - diff);
    }
}
