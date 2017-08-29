package com.lithium.dbi.rdbi.recipes.cache;

public interface KeyGenerator<KeyType> {
    String redisKey(KeyType key);
}
