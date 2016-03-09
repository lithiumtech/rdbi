package com.lithium.dbi.rdbi.ratelimiter;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RateLimiterConfiguration {

    @JsonProperty
    private String keyPrefix;

    @JsonProperty
    private String redisHostAndPort;

    @JsonProperty
    private Integer redisDb;

    @JsonProperty
    private Integer maxTotalConnections = 25;

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public String getRedisHostAndPort() {
        return redisHostAndPort;
    }

    public void setRedisHostAndPort(String redisHostAndPort) {
        this.redisHostAndPort = redisHostAndPort;
    }

    public Integer getRedisDb() {
        return redisDb;
    }

    public void setRedisDb(Integer redisDb) {
        this.redisDb = redisDb;
    }

    public Integer getMaxTotalConnections() {
        return maxTotalConnections;
    }
}
