package com.lithium.dbi.rdbi.recipes.cache;

public class CachedData <ValueType> {
    private final Long secondsToLive;
    private final ValueType data;

    public CachedData(Long secondsToLive, ValueType data) {
        this.secondsToLive = secondsToLive;
        this.data = data;
    }

    public Long getSecondsToLive() {
        return secondsToLive;
    }

    public ValueType getData() {
        return data;
    }
}
