package com.lithium.rdbi;

public interface ResultMapper<T> {
    T map(Object result);
}
