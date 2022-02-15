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
