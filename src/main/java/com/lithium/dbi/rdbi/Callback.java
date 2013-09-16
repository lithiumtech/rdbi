package com.lithium.dbi.rdbi;

public interface Callback<T> {
    T run(Handle handle);
}
