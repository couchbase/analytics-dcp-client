/*
 * Copyright (c) 2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.events;

import java.util.concurrent.TimeUnit;

import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.conductor.Conductor;

public class DeadConnectionDetection {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int ATTEMPT_TIMEOUT = 2000;
    private static final int TOTAL_TIMEOUT = 0;
    private static final Delay DELAY = Delay.fixed(0, TimeUnit.MILLISECONDS);

    private final Conductor conductor;
    private final long interval;
    private Span span;

    public DeadConnectionDetection(Conductor conductor) {
        this.conductor = conductor;
        this.interval = conductor.getEnv().getDeadConnectionDetectionInterval();
        span = Span.start(interval, TimeUnit.MILLISECONDS);
    }

    public void run() {
        if (span.elapsed()) {
            span = Span.start(interval, TimeUnit.MILLISECONDS);
            LOGGER.info("Running dead connection detection");
            conductor.reviveDeadConnections(ATTEMPT_TIMEOUT, TOTAL_TIMEOUT, DELAY);
        }
    }

    public long nanosTilNextCheck() {
        return span.remaining(TimeUnit.NANOSECONDS);
    }
}
