package com.lithium.dbi.rdbi.ratelimiter;

import java.time.Duration;
import java.util.OptionalLong;

public interface RateLimiter {
    /**
     * Non-blocking method
     *
     * @return whether the permit was acquired or not.
     */
    default boolean acquire() {
        return !getWaitTimeForPermit().isPresent();
    }

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
                final long timeAvailableToWait = timeout.toMillis() - timeWaited;
                final long timeToSleep = Math.min(timeAvailableToWait, waitTime.getAsLong());
                try {
                    Thread.sleep(timeToSleep);
                    timeWaited += timeToSleep;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
        return false;
    }

    /**
     * @return the key used by the rate limiter for logging
     */
    String getKey();
}
