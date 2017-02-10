/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
 */
package com.couchbase.client.dcp.util.retry;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

import rx.Observable;
import rx.functions.Func1;

/**
 * Combine a {@link Retry#errorsWithAttempts(Observable, int) mapping of errors to their attempt number} with
 * a flatmap that {@link RetryWithDelayHandler induces a retry delay} into a function that can be passed to
 * an Observable's {@link Observable#retryWhen(Func1) retryWhen operation}.
 *
 * @see RetryBuilder how to construct such a function in a fluent manner.
 *
 * @author Simon Baslé
 * @since 1.0.0
 */
@InterfaceStability.Committed
@InterfaceAudience.Public
public class RetryWhenFunction implements Func1<Observable<? extends Throwable>, Observable<?>> {

    protected RetryWithDelayHandler handler;

    public RetryWhenFunction(RetryWithDelayHandler handler) {
        this.handler = handler;
    }

    @Override
    public Observable<?> call(Observable<? extends Throwable> errors) {
        return Retry.errorsWithAttempts(errors, handler.maxAttempts + 1).flatMap(handler);
    }
}
