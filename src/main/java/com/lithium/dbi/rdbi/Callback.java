package com.lithium.dbi.rdbi;

/**
 * Given a RDBI handle, perform some transformation and return a result of type T.
 * @param <T>
 */
public interface Callback<T> {
    T run(Handle handle);
}
