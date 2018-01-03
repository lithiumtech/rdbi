package com.lithium.dbi.rdbi;

import java.util.function.LongSupplier;

public class TestClock implements LongSupplier {
    private final long interval;
    private long now;

    public TestClock(long start, long interval) {
        this.now = start;
        this.interval = interval;
    }

    public void tick() {
        now += interval;
    }

    @Override
    public long getAsLong() {
        return now;
    }
}
