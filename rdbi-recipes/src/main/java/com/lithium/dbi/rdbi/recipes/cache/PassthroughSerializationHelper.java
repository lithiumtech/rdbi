package com.lithium.dbi.rdbi.recipes.cache;

public class PassthroughSerializationHelper implements SerializationHelper<String> {

    @Override
    public String decode(String s) {
        return s;
    }

    @Override
    public String encode(String s) {
        return s;
    }
}
