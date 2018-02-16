package com.lithium.dbi.rdbi.recipes.cache;

import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "integration")
public class RedisMapTest {
    final String cachePrefix = "BLOPITYBLIPBLOP" + UUID.randomUUID().toString();

    @BeforeMethod
    public void clobberThings() {
        final RDBI rdbi = new RDBI(new JedisPool("localhost"));
        try (final Handle h = rdbi.open()) {
            final Set<String> keys = h.jedis().keys(cachePrefix + "*");
            final Pipeline pipeline = h.jedis().pipelined();
            for (final String key : keys) {
                pipeline.del(key);
            }
            pipeline.sync();
        }
    }

    @Test
    public void sniffTest() {
        final RDBI rdbi = new RDBI(new JedisPool("localhost"));

        final Map<String, RedisCacheTest.TestContainer> rMap = new RedisMap<>(RedisCacheTest.keyGenerator,
                                                                              RedisCacheTest.helper,
                                                                              rdbi,
                                                                              "mycache" + UUID.randomUUID().toString(),
                                                                              cachePrefix,
                                                                              Duration.ofSeconds(60));

        final RedisCacheTest.TestContainer value1 = new RedisCacheTest.TestContainer(UUID.randomUUID());
        final RedisCacheTest.TestContainer value2 = new RedisCacheTest.TestContainer(UUID.randomUUID());

        rMap.put("value1", value1);
        rMap.put("value2", value2);

        assertEquals(rMap.get("value1"), value1);
        assertEquals(rMap.get("value2"), value2);
    }

    @Test
    public void multiInstanceTest() {
        final RDBI rdbi = new RDBI(new JedisPool("localhost"));
        final String cacheName = "mycache" + UUID.randomUUID().toString();

        final Map<String, RedisCacheTest.TestContainer> rMap = new RedisMap<>(RedisCacheTest.keyGenerator,
                                                                              RedisCacheTest.helper,
                                                                              rdbi,
                                                                              cacheName,
                                                                              cachePrefix,
                                                                              Duration.ofSeconds(60));

        assertNull(rMap.get("key1"));
        assertNull(rMap.get("key2"));
        assertNull(rMap.get("key3"));

        final RedisCacheTest.TestContainer value1 = new RedisCacheTest.TestContainer(UUID.randomUUID());
        final RedisCacheTest.TestContainer value2 = new RedisCacheTest.TestContainer(UUID.randomUUID());

        rMap.put("key1", value1);
        rMap.put("key2", value2);

        assertEquals(rMap.get("key1"), value1);
        assertEquals(rMap.get("key2"), value2);
        assertNull(rMap.get("key3"));

        final Map<String, RedisCacheTest.TestContainer> rMap2 = new RedisMap<>(RedisCacheTest.keyGenerator,
                                                                               RedisCacheTest.helper,
                                                                               rdbi,
                                                                               cacheName,
                                                                               cachePrefix,
                                                                               Duration.ofSeconds(60));

        assertEquals(rMap2.get("key1"), value1);
        assertEquals(rMap2.get("key2"), value2);
        assertNull(rMap2.get("key3"));

        final RedisCacheTest.TestContainer value1dot2 = new RedisCacheTest.TestContainer(UUID.randomUUID());
        rMap2.put("key1", value1dot2);
        assertEquals(rMap2.get("key1"), value1dot2);
        assertEquals(rMap.get("key1"), value1dot2);
        assertEquals(rMap.remove("key1"), value1dot2);
        assertNull(rMap.get("key1"));
        assertFalse(rMap2.containsKey("key1"));
        assertTrue(rMap2.containsKey("key2"));
    }
}
