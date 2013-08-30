package com.lithium.rdbi;

public interface JedisCallback<T> {
    T run(JedisHandle handle);
}
