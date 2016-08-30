package com.lithium.dbi.rdbi.recipes.cache;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;

public interface SimpleSingleCache<T> {

    int DEFAULT_CACHE_TIME = 300;

    T loadWithFallback(String cacheKey,
                       Callable<T> loadFunction,
                       int cacheTimeToLive,
                       @Nullable CacheMetrics cacheMetrics) throws Exception;

    default T loadWithFallback(String cacheKey, Callable<T> loadFunction) throws Exception {
        return loadWithFallback(cacheKey, loadFunction, DEFAULT_CACHE_TIME, null);
    }
}
