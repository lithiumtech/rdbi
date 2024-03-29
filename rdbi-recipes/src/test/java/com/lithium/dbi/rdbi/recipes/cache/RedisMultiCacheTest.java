package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;

@Test(groups = "integration")
public class RedisMultiCacheTest {

    private static final String TEST_NAMESPACE = RedisMultiCacheTest.class.getSimpleName();

    private final short key1 = 1;
    private final short key2 = 2;
    private final short key3 = 3;

    @BeforeMethod
    public void clearRedis() {
        createRdbi().withHandle((handle) -> {
            for (String keyToPurge : handle.jedis().keys(TEST_NAMESPACE + "*")) {
                handle.jedis().del(keyToPurge);
            }
            return null;
        });
    }

    @Test
    public void sniffTest() {
        final CountingLoader loader = successLoader();
        final RedisMultiCache<Short, Long> cache = redisMultiCache(loader);

        assertEquals(loader.countLoadRequests(key1), 0);
        assertEquals(loader.countLoadRequests(key2), 0);
        assertEquals(loader.countLoadRequests(key3), 0);

        assertEquals(cache.get(ImmutableSet.of(key1, key2)),
                     ImmutableMap.of(key1, 2L, key2, 4L));

        assertEquals(loader.countLoadRequests(key1), 1);
        assertEquals(loader.countLoadRequests(key2), 1);
        assertEquals(loader.countLoadRequests(key3), 0);

        assertEquals(cache.get(ImmutableSet.of(key2, key3)),
                     ImmutableMap.of(key2, 4L, key3, 6L));

        assertEquals(loader.countLoadRequests(key1), 1);
        assertEquals(loader.countLoadRequests(key2), 1);
        assertEquals(loader.countLoadRequests(key3), 1);

        cache.get(ImmutableSet.of(key1, key2, key3));

        assertEquals(loader.countLoadRequests(key1), 1);
        assertEquals(loader.countLoadRequests(key2), 1);
        assertEquals(loader.countLoadRequests(key3), 1);
    }

    @Test
    public void partialLoadFailure() {
        final CountingLoader loader = partialSuccessLoader();
        final RedisMultiCache<Short, Long> cache = redisMultiCache(loader);

        assertEquals(loader.countLoadRequests(key1), 0);
        assertEquals(loader.countLoadRequests(key2), 0);
        assertEquals(loader.countLoadRequests(key3), 0);

        assertEquals(cache.get(ImmutableSet.of(key1, key2, key3)),
                     ImmutableMap.of(key1, 2L));

        assertEquals(loader.countLoadRequests(key1), 1);
        assertEquals(loader.countLoadRequests(key2), 1);
        assertEquals(loader.countLoadRequests(key3), 1);

        assertEquals(cache.get(ImmutableSet.of(key1, key2, key3)),
                     ImmutableMap.of(key1, 2L));

        assertEquals(loader.countLoadRequests(key1), 1);
        assertEquals(loader.countLoadRequests(key2), 2);
        assertEquals(loader.countLoadRequests(key3), 2);
    }

    @Test
    public void loadException() {
        final CountingLoader loader = exceptionLoader();
        final RedisMultiCache<Short, Long> cache = redisMultiCache(loader);

        assertEquals(loader.countLoadRequests(key1), 0);
        assertEquals(loader.countLoadRequests(key2), 0);
        assertEquals(loader.countLoadRequests(key3), 0);

        assertEquals(cache.get(ImmutableSet.of(key1, key2, key3)),
                     ImmutableMap.of());

        assertEquals(loader.countLoadRequests(key1), 1);
        assertEquals(loader.countLoadRequests(key2), 1);
        assertEquals(loader.countLoadRequests(key3), 1);

        assertEquals(cache.get(ImmutableSet.of(key1, key2, key3)),
                     ImmutableMap.of());

        assertEquals(loader.countLoadRequests(key1), 2);
        assertEquals(loader.countLoadRequests(key2), 2);
        assertEquals(loader.countLoadRequests(key3), 2);
    }

    @Test
    public void noneFound() {
        final CountingLoader loader = notFoundLoader();
        final RedisMultiCache<Short, Long> cache = redisMultiCache(loader);

        assertEquals(cache.get(ImmutableSet.of(key1, key2, key3)),
                     ImmutableMap.of());
    }

    @Test
    public void expiration() throws InterruptedException {
        final int ttlSeconds = 1;
        final CountingLoader loader = successLoader();
        final RedisMultiCache<Short, Long> cache = redisMultiCache(loader, ttlSeconds);

        // assert no cache interactions thus far
        assertEquals(loader.countLoadRequests(key1), 0);
        assertEquals(loader.countLoadRequests(key2), 0);

        // first access of key1 should be a cache miss
        cache.get(ImmutableSet.of(key1));
        assertEquals(loader.countLoadRequests(key1), 1);

        // second access of key1 should be a cache hit
        cache.get(ImmutableSet.of(key1));
        assertEquals(loader.countLoadRequests(key1), 1);

        // wait until near the expiration boundary
        TimeUnit.MILLISECONDS.sleep(700);

        // third access of key1 should still be a cache hit
        cache.get(ImmutableSet.of(key1));
        assertEquals(loader.countLoadRequests(key1), 1);

        // first access of key2 should be a cache miss
        cache.get(ImmutableSet.of(key2));
        assertEquals(loader.countLoadRequests(key2), 1);

        // wait to extend beyond the expiration boundary for key1
        TimeUnit.MILLISECONDS.sleep(301);

        // fourth access of key1 should be a cache miss again
        cache.get(ImmutableSet.of(key1));
        assertEquals(loader.countLoadRequests(key1), 2);

        // but next access of key2 should still be a cache hit
        cache.get(ImmutableSet.of(key2));
        assertEquals(loader.countLoadRequests(key2), 1);
    }

    private RedisMultiCache<Short, Long> redisMultiCache(CountingLoader loader) {
        return redisMultiCache(loader, 30);
    }

    private RedisMultiCache<Short, Long> redisMultiCache(CountingLoader loader, int ttlSeconds) {
        return new RedisMultiCache<>(createRdbi(),
                                     TEST_NAMESPACE,
                                     ttlSeconds,
                                     fieldGenerator(),
                                     loader,
                                     valueSerializer(),
                                     valueKeyGenerator());
    }

    private SerializationHelper<Long> valueSerializer() {
        return new SerializationHelper<Long>() {
            @Override
            public Long decode(String encoded) {
                return Long.parseLong(encoded);
            }

            @Override
            public String encode(Long value) {
                return value.toString();
            }
        };
    }

    private Function<Short, String> fieldGenerator() {
        return Object::toString;
    }

    private Function<Long, Short> valueKeyGenerator() {
        return value -> (short) (value / 2);
    }

    private CountingLoader successLoader() {
        return new CountingLoader() {
            @Override
            public Collection<Long> apply(Set<Short> keys) {
                final List<Long> values = new ArrayList<>();
                for (Short key : keys) {
                    values.add(load(key));
                }
                return values;
            }
        };
    }

    private CountingLoader partialSuccessLoader() {
        return new CountingLoader() {
            @Override
            public Collection<Long> apply(Set<Short> keys) {
                final List<Long> values = new ArrayList<>();
                for (Short key : keys) {
                    final Long value = load(key);
                    if (key % 2 == 1) {
                        values.add((key == 3) ? null : value);
                    }
                }
                return values;
            }
        };
    }

    private CountingLoader exceptionLoader() {
        return new CountingLoader() {
            @Override
            public Collection<Long> apply(Set<Short> keys) {
                for (Short key : keys) {
                    load(key);
                }
                throw new RuntimeException("oh-noes!");
            }
        };
    }

    private CountingLoader notFoundLoader() {
        return new CountingLoader() {

            protected Long load(Short key) {
                return null;
            }

            @Override
            public Collection<Long> apply(Set<Short> keys) {
                final List<Long> values = new ArrayList<>();
                for (Short key : keys) {
                    values.add(load(key));
                }
                return values;
            }
        };
    }

    private RDBI createRdbi() {
        return new RDBI(new JedisPool("localhost", 6379));
    }

    private static abstract class CountingLoader implements Function<Set<Short>, Collection<Long>> {

        private final Map<Short, Integer> loadCounts = new HashMap<>();

        private int countLoadRequests(Short key) {
            return MoreObjects.firstNonNull(loadCounts.get(key), 0);
        }

        protected Long load(Short key) {
            loadCounts.put(key, MoreObjects.firstNonNull(loadCounts.get(key), 0) + 1);
            return key * 2L;
        }
    }

}
