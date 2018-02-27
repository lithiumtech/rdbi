package com.lithium.dbi.rdbi.recipes.cache;

import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "integration")
public class RedisCacheInvalidateTest {

    private static JedisPool jedisPool;
    private static RedisCache<String, Integer> cache;
    private static int loadDelayMillis;
    private static AtomicInteger sharedMutableState;

    @BeforeClass
    public static void setUp() throws Exception {
        jedisPool = new JedisPool("localhost");

        final RDBI rdbi = new RDBI(jedisPool);

        final Runnable noop = new Runnable() {
            @Override
            public void run() {
            }
        };

        final KeyGenerator<String> keyGenerator = new KeyGenerator<String>() {
            @Override
            public String redisKey(String key) {
                return key;
            }
        };

        final SerializationHelper<Integer> serializationHelper = new SerializationHelper<Integer>() {
            @Override
            public Integer decode(String string) {
                return Integer.valueOf(string);
            }

            @Override
            public String encode(Integer value) {
                return value.toString();
            }
        };

        sharedMutableState = new AtomicInteger(0);
        loadDelayMillis = 100;
        final Function<String, Integer> loader = key -> {
                int readState = sharedMutableState.get();
                sleep(loadDelayMillis);
                return readState;
        };

        final ExecutorService exeService = Executors.newCachedThreadPool();
        cache = new RedisCache<>(keyGenerator,
                                 serializationHelper,
                                 rdbi,
                                 loader,
                                 RedisCacheInvalidateTest.class.getName(),
                                 RedisCacheInvalidateTest.class.getSimpleName() + ":" + UUID.randomUUID().toString(),
                                 60,
                                 60 - 30,
                                 60,
                                 Optional.of(exeService),
                                 noop,
                                 noop,
                                 noop,
                                 noop);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        jedisPool.close();
    }

    @Test
    public void testAcquireLockPatiently() {
        final String key = "baz";
        try {
            cache.acquireLock(key);
            assertFalse(cache.acquireLockPatiently(key, 300),
                        "lock already acquired, so shouldn't be able to get it patiently");

            cache.releaseLock(key);
            assertTrue(cache.acquireLockPatiently(key, 300),
                        "lock already acquired, so shouldn't be able to get it patiently");
        } finally {
            cache.releaseLock(key);
        }
    }

    @Test
    public void testInvalidatePatientlyThenForcibly() throws Exception {

        final String key = "foo";

        assertEquals("first load of cache key gets the initial value",
                     0, cache.getPatiently(key, 30_000).getOrThrowUnchecked().intValue());

        // begin an async refresh
        cache.refresh(key);
        sleep(loadDelayMillis / 2);

        // value is changed and cache marks key invalid
        sharedMutableState.incrementAndGet();
        // a call to cache .invalidate(key) is insufficient here to avoid a subsequent dirty read
        cache.invalidatePatientlyThenForcibly(key, loadDelayMillis * 2);

        // sleep long enough for the in-progress async refresh to finish
        sleep(loadDelayMillis * 2);


        assertEquals("cache was invalidated, so next read should not be dirty",
                     1, cache.getPatiently(key, 30_000).getOrThrowUnchecked().intValue());
    }

    private static void sleep(int millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            fail("interrupted");
        }
    }

}
