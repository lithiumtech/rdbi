package com.lithium.dbi.rdbi.recipes.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Callable;

public class AsyncCacheAllRefresher<KeyType, ValueType> implements Callable<CallbackResult<Collection<ValueType>>> {

    private static final Logger log = LoggerFactory.getLogger(AsyncCacheAllRefresher.class);

    private final LoadAllCache<KeyType, ValueType> cache;

    protected AsyncCacheAllRefresher(final LoadAllCache<KeyType, ValueType> cache) {
        this.cache = cache;
    }

    @Override
    public CallbackResult<Collection<ValueType>> call() {
        final long start = System.currentTimeMillis();

        if(!cache.acquireLock()) {
            log.debug("{}: Unable to acquire refresh lock", cache.getCacheName());
            cache.markLoadException(System.currentTimeMillis() - start);
            return new CallbackResult<>(new LockUnavailableException());
        }

        log.debug("{}: Attempting to refresh data", cache.getCacheName());

        try {
            final Collection<ValueType> values = cache.fetchAll();
            if (values == null) {
                cache.markLoadException(System.currentTimeMillis() - start);
                return new CallbackResult<>();
            }

            log.info("{}: Async refresh", cache.getCacheName());
            cache.applyFetchAll(values);
            cache.markLoadSuccess(System.currentTimeMillis() - start);
            cache.loadAllComplete();
            return new CallbackResult<>(values);
        } catch (Exception ex) {
            cache.markLoadException(System.currentTimeMillis() - start);
            return new CallbackResult<>(ex);
        } finally {
            cache.releaseLock();
        }
    }
}
