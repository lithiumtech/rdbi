package com.lithium.dbi.rdbi.ratelimiter;

import java.util.OptionalLong;


public interface Limiter {
    /**
     * @return Whether the permit was acquired or not.
     */
    boolean acquire();

    /**
     * @return If absent, the permit has been acquired. If present, indicates the time the client should wait before
     * attempting to acquire permit again.
     */
    OptionalLong getOptionalWaitTimeForPermit();
}
