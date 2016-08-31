package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.cache.LoadingCache;

public interface InstrumentedCache<KeyType, ValueType> extends LoadingCache<KeyType, ValueType> {
    void markLoadException(final long loadTime);
    void markLoadSuccess(final long loadTime);
    void markMiss();
    void markHit();
    String getCacheName();
    ValueType load(KeyType key);
}
