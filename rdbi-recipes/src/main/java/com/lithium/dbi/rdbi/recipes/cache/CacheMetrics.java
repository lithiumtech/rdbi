package com.lithium.dbi.rdbi.recipes.cache;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class CacheMetrics {

    private final Meter cacheHits;
    private final Meter cacheMisses;
    private final Timer loadTimer;

    public CacheMetrics(MetricRegistry metricRegistry, Class<?> clazz, String name) {
        this.cacheHits = metricRegistry.meter(MetricRegistry.name(clazz, name, "cacheHits"));
        this.cacheMisses = metricRegistry.meter(MetricRegistry.name(clazz, name, "cacheMisses"));
        this.loadTimer = metricRegistry.timer(MetricRegistry.name(clazz, name, "loadTime"));
    }

    public void markHit() {
        cacheHits.mark();
    }

    public long countHits() {
        return cacheHits.getCount();
    }

    public void markMiss() {
        cacheMisses.mark();
    }

    public long countMisses() {
        return cacheMisses.getCount();
    }

    public Timer.Context time() {
        return loadTimer.time();
    }
}
