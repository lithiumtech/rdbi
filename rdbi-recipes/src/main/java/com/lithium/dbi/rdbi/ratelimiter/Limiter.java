package com.lithium.dbi.rdbi.ratelimiter;

import com.google.common.base.Throwables;

import java.time.Duration;
import java.util.OptionalLong;


public interface Limiter {
    /**
     * Non-blocking method
     *
     * @return whether the permit was acquired or not.
     */
    boolean acquire();

    /**
     * Non blocking method
     *
     * @return If absent, the permit has been acquired. If present, indicates the time the client should wait before
     * attempting to acquire permit again.
     */
    OptionalLong getWaitTimeForPermit();

    /**
     * Blocking method
     * @param timeout
     * @return true if acquired within the timeout, or false otherwise
     */
    default boolean acquirePatiently(Duration timeout) {
        long timeWaited = 0L;
        while (timeWaited < timeout.toMillis()) {
            final OptionalLong waitTime = getWaitTimeForPermit();
            if (!waitTime.isPresent()) {
                return true;
            } else {
                final long timeToSleep = Math.max(100, waitTime.getAsLong());
                try {
                    Thread.sleep(timeToSleep);
                    timeWaited += timeToSleep;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
            }
        }
        return false;
    }
}
