package com.lithium.rdbi;

public interface RedisResultMapper<T> {
    T map(Object result);
}
