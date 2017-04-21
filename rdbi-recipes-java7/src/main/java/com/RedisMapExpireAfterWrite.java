package com;

import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.recipes.cache.KeyGenerator;
import com.lithium.dbi.rdbi.recipes.cache.RedisMap;
import com.lithium.dbi.rdbi.recipes.cache.SerializationHelper;
import org.joda.time.Duration;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class RedisMapExpireAfterWrite<KeyType, ValueType> extends RedisMap<KeyType, ValueType> {
    /**
     * @param redisKeyGenerator   - something that will turn your key object into a string redis can use as a key.
     *                            will be prefixed by the keyPrefix string.
     * @param serializationHelper - a codec to get your value object to and from a string
     * @param rdbi                - RDBI instance to use.
     * @param cacheName           - name of cache, used in log statements
     * @param keyPrefix           - prefix of all keys used by this cache in redis
     * @param valueTtl            - redis entries holding your values will expire this many seconds after write
     */
    public RedisMapExpireAfterWrite(final KeyGenerator<KeyType> redisKeyGenerator,
                                    final SerializationHelper<ValueType> serializationHelper,
                                    final RDBI rdbi,
                                    final String cacheName,
                                    final String keyPrefix,
                                    final Duration valueTtl) {
        super(redisKeyGenerator, serializationHelper, rdbi, cacheName, keyPrefix, valueTtl);
    }

    @Override
    public ValueType get(Object key) {
        final String redisKey = generateRedisKey(turnObjectIntoKeyType(key));
        final Response<String> valAsString;
        try (final Handle handle = rdbi.open()) {
            final Pipeline pl = handle.jedis().pipelined();
            valAsString = pl.get(redisKey);
            pl.sync();
        }
        return getFromResponse(valAsString);
    }
}
