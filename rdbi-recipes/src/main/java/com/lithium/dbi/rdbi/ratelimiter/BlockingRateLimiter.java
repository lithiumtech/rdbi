package com.lithium.dbi.rdbi.ratelimiter;

import com.google.common.base.Throwables;

import java.util.OptionalLong;

public class BlockingRateLimiter implements Limiter {
    private final Limiter delegate;

    public BlockingRateLimiter(Limiter delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean acquire() {
        return !getOptionalWaitTimeForPermit().isPresent();
    }

    @Override
    public OptionalLong getOptionalWaitTimeForPermit() {
        while(true) {
            final OptionalLong waitTime = delegate.getOptionalWaitTimeForPermit();
            if (!waitTime.isPresent()) {
                return waitTime;
            } else {
                try {
                    Thread.sleep(Math.max(100, waitTime.getAsLong()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
            }
        }
    }
}
