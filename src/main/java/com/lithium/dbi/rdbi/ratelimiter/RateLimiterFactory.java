package com.lithium.dbi.rdbi.ratelimiter;

import com.lithium.dbi.rdbi.RDBI;

public class RateLimiterFactory {

    private final RateLimiterConfiguration rateLimiterConfiguration;
    private final RDBI rdbi;

    public RateLimiterFactory(RateLimiterConfiguration rateLimiterConfiguration, RDBI rdbi) {
        this.rateLimiterConfiguration = rateLimiterConfiguration;
        this.rdbi = rdbi;
    }

    public RateLimiter getInstance(String key, double permitsPerSecond) {
        return new RateLimiter(rateLimiterConfiguration, rdbi, key, permitsPerSecond);
    }
}
