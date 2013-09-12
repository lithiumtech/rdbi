package com.lithium.rdbi;

public interface Callback<T> {
    T run(Handle handle);
}
