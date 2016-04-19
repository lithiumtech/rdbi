package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;

/**
 * A loading cache for apis that prefer to load multiple keys in a single round trip.
 * <p/>
 * Contrast this cache with {@link RedisHashCache}, which is also a hash-based redis-backed cache.  Whereas {@link RedisHashCache}
 * fetches data for missed cached keys iteratively, this {@link RedisMultiCache} expects to resolve multiple cache misses with a single
 * call to a bulk loader.  Moreover, this cache makes no assumptions about being able to load values for all of the caches potential keys.
 * <p/>
 * This cache is <b>not</b> an automatically refreshing cache.  After a key expires, the next request for the key may
 * incur some loading penalty.
 */
public class RedisMultiCache<KeyType, ValueType> {

    private static final Logger log = LoggerFactory.getLogger(RedisMultiCache.class);

    private final RDBI rdbi;
    private final String cacheName;
    private final int cacheTtlSeconds;
    private final Function<KeyType, String> redisFieldGenerator;
    private final Function<Set<KeyType>, Collection<ValueType>> loader;
    private final SerializationHelper<ValueType> valueTypeSerializationHelper;
    private final Function<ValueType, KeyType> valueKeyGenerator;


    public RedisMultiCache(RDBI rdbi,
                           String cacheName,
                           int cacheTtlSeconds,
                           Function<KeyType, String> redisFieldGenerator,
                           Function<Set<KeyType>, Collection<ValueType>> loader,
                           SerializationHelper<ValueType> valueTypeSerializationHelper,
                           Function<ValueType, KeyType> valueKeyGenerator) {
        this.rdbi = rdbi;
        this.cacheName = cacheName;
        this.cacheTtlSeconds = cacheTtlSeconds;
        this.redisFieldGenerator = redisFieldGenerator;
        this.loader = loader;
        this.valueTypeSerializationHelper = valueTypeSerializationHelper;
        this.valueKeyGenerator = valueKeyGenerator;
    }

    public Map<KeyType, ValueType> get(Set<KeyType> keys) {
        final Map<KeyType, ValueType> hits = getCacheHits(keys);
        final Set<KeyType> misses = Sets.difference(keys, hits.keySet());
        final Map<KeyType, ValueType> resolvedMisses = resolveCacheMisses(misses);
        return ImmutableMap.<KeyType, ValueType>builder().putAll(hits).putAll(resolvedMisses).build();
    }

    private Map<KeyType, ValueType> getCacheHits(Set<KeyType> keys) {
        try (Handle handle = rdbi.open()) {
            final String[] fields = generateRedisFields(keys);
            final List<String> encodedHits = handle.jedis().hmget(cacheName, fields);
            return FluentIterable.from(encodedHits)
                                 .transform(decodeValue())
                                 .filter(Predicates.notNull())
                                 .uniqueIndex(valueKeyGenerator);
        } catch (Exception ex) {
            log.error("{}: failed to fetch values from cache", cacheName, ex);
            return ImmutableMap.of();
        }
    }

    private String[] generateRedisFields(Iterable<KeyType> keys) {
        final Set<String> redisHashFields = new HashSet<>();
        for (KeyType key : keys) {
            final String redisHashField = redisFieldGenerator.apply(key);
            if (redisHashField != null) {
                redisHashFields.add(redisHashField);
            }
        }
        return redisHashFields.toArray(new String[redisHashFields.size()]);
    }

    private Function<String, ValueType> decodeValue() {
        return new Function<String, ValueType>() {
            @Nullable
            @Override
            public ValueType apply(@Nullable String encodedValue) {
                if (encodedValue != null) {
                    try {
                        return valueTypeSerializationHelper.decode(encodedValue);
                    } catch (Exception ex) {
                        log.error("{}: failed to decode cached value: {}", cacheName, encodedValue, ex);
                    }
                }
                return null;
            }
        };
    }

    private Map<KeyType, ValueType> resolveCacheMisses(Set<KeyType> keys) {
        final Map<KeyType, ValueType> resolvedMisses;
        try {
            resolvedMisses = FluentIterable.from(firstNonNull(loader.apply(keys), ImmutableList.<ValueType>of()))
                                           .filter(Predicates.notNull())
                                           .uniqueIndex(valueKeyGenerator);
        } catch (Exception ex) {
            log.error("{}: failed to resolve cache misses", cacheName, ex);
            return ImmutableMap.of();
        }

        if(!resolvedMisses.isEmpty()) {
            try (Handle handle = rdbi.open()) {
                handle.jedis().hmset(cacheName, generateCachePayload(resolvedMisses));
                handle.jedis().expire(cacheName, cacheTtlSeconds);
            } catch (Exception ex) {
                log.error("{}: failed to persist resolved cache misses", cacheName, ex);
            }
        }

        return resolvedMisses;
    }

    private Map<String, String> generateCachePayload(Map<KeyType, ValueType> keysAndValues) {
        final Map<String, String> cachePayload = new HashMap<>();
        for (Map.Entry<KeyType, ValueType> keyAndValue : keysAndValues.entrySet()) {
            final String redisField = redisFieldGenerator.apply(keyAndValue.getKey());
            final String encodedValue = valueTypeSerializationHelper.encode(keyAndValue.getValue());
            if (redisField != null && encodedValue != null) {
                cachePayload.put(redisField, encodedValue);
            }
        }
        return cachePayload;
    }


}
