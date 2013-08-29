package com.lithium.rdbi;

public interface RDBICallback<T> {
    T run(JedisHandle handle);
}
