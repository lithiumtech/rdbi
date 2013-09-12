package com.lithium.rdbi;

//CR: Would love to see this interface generify the "result" parameter on the map method.
public interface ResultMapper<T> {
    T map(Object result);
}
