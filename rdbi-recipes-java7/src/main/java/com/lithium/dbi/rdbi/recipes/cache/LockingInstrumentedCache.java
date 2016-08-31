package com.lithium.dbi.rdbi.recipes.cache;

public interface LockingInstrumentedCache<KeyType, ValueType> extends InstrumentedCache<KeyType, ValueType> {
    boolean acquireLock(final KeyType key);
    void releaseLock(final KeyType key);
}
