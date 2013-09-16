package com.lithium.dbi.rdbi;

//CR: Would love to see this interface generify the "result" parameter on the map method.
public interface ResultMapper<T, S> {
    T map(S result);
}
