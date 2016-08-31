package com.lithium.dbi.rdbi;

/**
 * Given a RDBI handle, perform some transformation and return a result of type T.
 * @param <T> the generic type returned by this callback.
 */
public interface Callback<T> {
    T run(Handle handle);
}
