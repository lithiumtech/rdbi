package com.lithium.dbi.rdbi.recipes.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lithium.dbi.rdbi.RDBI;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;

/**
 * Implementation of {@link SimpleSingleCache} backed by a {@link SimpleMultiCacheRedis}
 * @param <T>
 */
public class SimpleSingleCacheRedis<T> implements SimpleSingleCache<T> {

    private final SimpleMultiCache multiCache;
    private final TypeReference<T> typeReference;

    public SimpleSingleCacheRedis(ObjectMapper jsonMapper, RDBI rdbi, TypeReference<T> typeReference) {
        this.multiCache = new SimpleMultiCacheRedis(jsonMapper, rdbi);
        this.typeReference = typeReference;
    }

    @Override
    public T loadWithFallback(String cacheKey, Callable<T> loadFunction, int cacheTimeToLive, @Nullable CacheMetrics cacheMetrics) throws Exception {
        return multiCache.loadWithFallback(cacheKey, typeReference, loadFunction, cacheTimeToLive, cacheMetrics);
    }
}
