package com.lithium.dbi.rdbi.recipes.cache;

public interface SerializationHelper<ValueType> {
    public ValueType decode(final String string);
    public String encode(final ValueType value);
}
