package com.lithium.dbi.rdbi.recipes.cache;

import com.fasterxml.jackson.core.type.TypeReference;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;

/**
 * A very simple caching layer that support multiple heterogeneous objects,
 * with some metrics about cache hits, misses, and load times.
 */
public interface SimpleMultiCache {

    int DEFAULT_CACHE_TIME = 300;

    <T> T loadWithFallback(String cacheKey,
                           TypeReference<T> clazz,
                           Callable<T> loadFunction,
                           int cacheTimeToLive,
                           @Nullable CacheMetrics cacheMetrics) throws Exception;

    default <T> T loadWithFallback(String cacheKey, TypeReference<T> clazz, Callable<T> loadFunction) throws Exception {
        return loadWithFallback(cacheKey, clazz, loadFunction, DEFAULT_CACHE_TIME, null);
    }
}
