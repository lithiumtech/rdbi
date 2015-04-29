package com.lithium.dbi.rdbi.recipes.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class AsyncLockFreeRefresher<KeyType, ValueType> implements Callable<CallbackResult<ValueType>> {

    private static final Logger log = LoggerFactory.getLogger(AsyncLockFreeRefresher.class);

    private final InstrumentedCache<KeyType, ValueType> cache;
    private final KeyType key;

    protected AsyncLockFreeRefresher(
            final InstrumentedCache<KeyType, ValueType> cache,
            final KeyType key) {
        this.cache = cache;
        this.key = key;
    }

    @Override
    public CallbackResult<ValueType> call() {
        final long start = System.currentTimeMillis();

        log.debug("{}: Attempting to refresh data for", cache.getCacheName());

        try {
            final ValueType value = cache.load(key);
            if (value == null) {
                cache.markLoadException(System.currentTimeMillis() - start);
                return new CallbackResult<>();
            }

            log.info("{}: Async refresh for {}", cache.getCacheName());
            cache.put(key, value); // this shouldn't throw, the withHandle eats it...
            cache.markLoadSuccess(System.currentTimeMillis() - start);
            return new CallbackResult<>(value);
        } catch (Exception ex) {
            cache.markLoadException(System.currentTimeMillis() - start);
            return new CallbackResult<>(ex);
        }
    }
}
