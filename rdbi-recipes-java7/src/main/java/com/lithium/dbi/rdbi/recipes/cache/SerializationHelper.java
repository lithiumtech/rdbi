package com.lithium.dbi.rdbi.recipes.cache;

public interface SerializationHelper<T> {
    public T decode(final String string);
    public String encode(final T value);
}
