/*
 * Copyright (c) 2017-2021 Couchbase, Inc.
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
    private Span span;

    public DeadConnectionDetection(Conductor conductor) {
        this.conductor = conductor;
        int intervalSecs = conductor.getEnv().getDeadConnectionDetectionIntervalSeconds();
        span = intervalSecs == 0 ? Span.INFINITE : Span.start(intervalSecs, TimeUnit.SECONDS);
    }

    public void run() {
        if (span.elapsed()) {
            LOGGER.info("Running dead connection detection");
            conductor.reviveDeadConnections(ATTEMPT_TIMEOUT, TOTAL_TIMEOUT, DELAY);
            span.reset();
        }
    }

    public long nanosTilNextCheck() {
        return span.remaining(TimeUnit.NANOSECONDS);
    }
}
