package com.lithium.dbi.rdbi.recipes.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class AsyncCacheRefresher<KeyType, ValueType> implements Callable<CallbackResult<ValueType>> {

    private static final Logger log = LoggerFactory.getLogger(AsyncCacheRefresher.class);

    private final LockingInstrumentedCache<KeyType, ValueType> cache;
    private final KeyType key;

    protected AsyncCacheRefresher(
            final LockingInstrumentedCache<KeyType, ValueType> cache,
            final KeyType key) {
        this.cache = cache;
        this.key = key;
    }

    @Override
    public CallbackResult<ValueType> call() {
        final long start = System.currentTimeMillis();

        if(!cache.acquireLock(key)) {
            log.debug("{}: Unable to acquire refresh lock for {}", cache.getCacheName(), key);
            cache.markLoadException(System.currentTimeMillis() - start);
            return new CallbackResult<>(new LockUnavailableException());
        }

        log.debug("{}: Attempting to refresh data for {}", cache.getCacheName(), key);

        try {
            final ValueType value = cache.load(key);
            if (value == null) {
                cache.markLoadException(System.currentTimeMillis() - start);
                return new CallbackResult<>();
            }

            log.info("{}: Async refresh for {}", cache.getCacheName(), key);
            cache.put(key, value); // this shouldn't throw, the withHandle eats it...
            cache.markLoadSuccess(System.currentTimeMillis() - start);
            return new CallbackResult<>(value);
        } catch (Exception ex) {
            cache.markLoadException(System.currentTimeMillis() - start);
            return new CallbackResult<>(ex);
        } finally {
            cache.releaseLock(key);
        }
    }
}
