package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.cache.CacheStats;
import com.lithium.dbi.rdbi.RDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public abstract class AbstractRedisCache<KeyType, ValueType> implements InstrumentedCache<KeyType, ValueType> {

    private static final Logger log = LoggerFactory.getLogger(AbstractRedisCache.class);

    private final Function<KeyType, ValueType> loader;

    protected final RDBI rdbi;
    protected final String cacheName;
    protected final Runnable hitAction;
    protected final Runnable missAction;
    protected final Runnable loadSuccessAction;
    protected final Runnable loadExceptionAction;
    protected final Optional<ExecutorService> asyncService;
    protected final AtomicLong cacheEvictionCount;
    protected final AtomicLong cacheHitCount;
    protected final AtomicLong cacheLoadExceptionCount;
    protected final AtomicLong cacheLoadSuccessCount;
    protected final AtomicLong cacheMissCount;
    protected final AtomicLong cacheTotalLoadTime;

    public AbstractRedisCache(
            String cacheName,
            Function<KeyType, ValueType> loader,
            RDBI rdbi,
            Optional<ExecutorService> asyncService,
            Runnable hitAction,
            Runnable missAction,
            Runnable loadSuccessAction,
            Runnable loadExceptionAction) {
        this.cacheName = cacheName;
        this.loader = loader;
        this.rdbi = rdbi;
        this.asyncService = asyncService;
        this.hitAction = hitAction;
        this.missAction = missAction;
        this.loadSuccessAction = loadSuccessAction;
        this.loadExceptionAction = loadExceptionAction;

        this.cacheHitCount = new AtomicLong();
        this.cacheMissCount = new AtomicLong();
        this.cacheLoadSuccessCount = new AtomicLong();
        this.cacheLoadExceptionCount = new AtomicLong();
        this.cacheTotalLoadTime = new AtomicLong();
        this.cacheEvictionCount = new AtomicLong();
    }

    @Override
    public CacheStats stats() {
        return new CacheStats(
                cacheHitCount.get(),
                cacheMissCount.get(),
                cacheLoadSuccessCount.get(),
                cacheLoadExceptionCount.get(),
                cacheTotalLoadTime.get(),
                cacheEvictionCount.get());
    }

    @Override
    public String getCacheName() {
        return cacheName;
    }

    @Override
    public ValueType load(KeyType key) {
        return loader.apply(key);
    }

    @Override
    public void markHit() {
        cacheHitCount.incrementAndGet();
        try {
            hitAction.run();
        } catch (Exception ex) {
            log.warn("{}: ex in callback", getCacheName(), ex);
        }
    }

    @Override
    public void markMiss() {
        cacheMissCount.incrementAndGet();
        try {
            missAction.run();
        } catch (Exception ex) {
            log.warn("{}: ex in callback", getCacheName(), ex);
        }
    }

    @Override
    public void markLoadException(final long loadTime) {
        cacheLoadExceptionCount.incrementAndGet();
        cacheTotalLoadTime.addAndGet(loadTime);
        try {
            loadExceptionAction.run();
        } catch (Exception ex) {
            log.warn("{}: ex in callback", getCacheName(), ex);
        }
    }

    @Override
    public void markLoadSuccess(final long loadTime) {
        cacheLoadSuccessCount.incrementAndGet();
        cacheTotalLoadTime.addAndGet(loadTime);
        try {
            loadSuccessAction.run();
        } catch (Exception ex) {
            log.warn("{}: ex in callback", getCacheName(), ex);
        }
    }


    abstract public CallbackResult<ValueType> getCallback(final KeyType key);

    @Override
    public ValueType apply(KeyType key) {
        return getUnchecked(key);
    }

    @Override
    public ValueType get(KeyType key) throws ExecutionException {
        try {
            return getCallback(key).getOrThrowUnchecked();
        } catch (Exception ex) {
            // markLoadException should have been called already
            throw new ExecutionException(ex);
        }
    }

    @Override
    public ValueType getUnchecked(KeyType key) {
        // We don't throw checked exceptions in this class.
        return getCallback(key).getOrThrowUnchecked();
    }
}

