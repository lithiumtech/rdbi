package com.lithium.dbi.rdbi.recipes.cache;

public interface SerializationHelper<T> {
    T decode(final String string);
    String encode(final T value);
}
