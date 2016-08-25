package com.lithium.dbi.rdbi.recipes.cache;

import java.util.Collection;

public interface LoadAllCache<KeyType, ValueType> extends InstrumentedCache<KeyType, ValueType> {

    /**
     * Attempt to acquire a global (for this cacheKey) update lock.
     * @return
     */
    boolean acquireLock();

    /**
     * Release any global (for this cacheKey) update lock.
     */
    void releaseLock();

    /**
     * Derive a KeyType from a ValueType.
     * @param value
     * @return
     */
    KeyType keyFromValue(ValueType value);

    /**
     * Fetches all available data from the original source.
     * @return
     * @throws Exception
     */
    Collection<ValueType> fetchAll() throws Exception;

    /**
     * Apply the most recently fetched data. Evicts previously cached, but since deleted keys.
     */
    void applyFetchAll(Collection<ValueType> fetched);

    /**
     * Perform some cleanup to indicate that all available data has been loaded.
     */
    void loadAllComplete();
}
