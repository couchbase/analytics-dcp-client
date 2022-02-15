/*
 * Copyright 2016-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */
package com.couchbase.client.dcp.util.retry;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;

import com.couchbase.client.core.time.Delay;

import rx.Observable;
import rx.functions.Func2;

/**
 * Utility methods to deal with retrying {@link Observable}s.
 *
 * @author Simon Baslé
 * @since 1.0.0
 */
public class Retry {

    public static final Delay DEFAULT_DELAY = Delay.fixed(1, TimeUnit.MILLISECONDS);

    private Retry() {
    }

    /**
     * Wrap an {@link Observable} so that it will retry on all errors for a maximum number of times.
     * The retry is almost immediate (1ms delay).
     *
     * @param source
     *            the {@link Observable} to wrap.
     * @param maxAttempts
     *            the maximum number of times to attempt a retry. It will be capped at <code>{@link Integer#MAX_VALUE} - 1</code>.
     * @param <T>
     *            the type of items emitted by the source Observable.
     * @return the wrapped retrying Observable.
     */
    public static <T> Observable<T> wrapForRetry(Observable<T> source, int maxAttempts) {
        return wrapForRetry(source, new RetryWithDelayHandler(maxAttempts, DEFAULT_DELAY));
    }

    /**
     * Wrap an {@link Observable} so that it will retry on all errors. The retry will occur for a maximum number of
     * attempts and with a provided {@link Delay} between each attempt.
     *
     * @param source
     *            the {@link Observable} to wrap.
     * @param maxAttempts
     *            the maximum number of times to attempt a retry. It will be capped at <code>{@link Integer#MAX_VALUE} - 1</code>.
     * @param retryDelay
     *            the {@link Delay} between each attempt.
     * @param <T>
     *            the type of items emitted by the source Observable.
     * @return the wrapped retrying Observable.
     */
    public static <T> Observable<T> wrapForRetry(Observable<T> source, int maxAttempts, Delay retryDelay) {
        return wrapForRetry(source, new RetryWithDelayHandler(maxAttempts, retryDelay));
    }

    /**
     * Wrap an {@link Observable} so that it will retry on some errors. The retry will occur for a maximum number of
     * attempts and with a provided {@link Delay} between each attempt represented by the {@link RetryWithDelayHandler},
     * which can also filter on errors and stop the retry cycle for certain type of errors.
     *
     * @param source
     *            the {@link Observable} to wrap.
     * @param handler
     *            the {@link RetryWithDelayHandler}, describes maximum number of attempts, delay and fatal errors.
     * @param <T>
     *            the type of items emitted by the source Observable.
     * @return the wrapped retrying Observable.
     */
    public static <T> Observable<T> wrapForRetry(Observable<T> source, final RetryWithDelayHandler handler) {
        return source.retryWhen(new RetryWhenFunction(handler));
    }

    /**
     * Internal utility method to combine errors in an observable with their attempt number.
     *
     * @param errors
     *            the errors.
     * @param expectedAttempts
     *            the maximum of combinations to make (for retry, should be the maximum number of
     *            authorized retries + 1).
     * @return an Observable that combines the index/attempt number of each error with its error in a {@link Pair}.
     */
    protected static Observable<Pair<Integer, Throwable>> errorsWithAttempts(Observable<? extends Throwable> errors,
            final int expectedAttempts) {
        return errors.zipWith(Observable.range(1, expectedAttempts),
                (Func2<Throwable, Integer, Pair<Integer, Throwable>>) (error, attempt) -> Pair.of(attempt, error));
    }

}
