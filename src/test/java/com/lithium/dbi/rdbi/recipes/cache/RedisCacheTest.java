package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.Assert.assertTrue;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "integration")
public class RedisCacheTest {
    public static class TestContainer {
        private final UUID uuid;

        public TestContainer(UUID uuid) {
            this.uuid = uuid;
        }

        public UUID getUuid() {
            return uuid;
        }
    }

    public static class CounterRunnable implements Runnable {
        private AtomicLong atomicLong = new AtomicLong();

        @Override
        public void run() {
            atomicLong.getAndIncrement();
        }

        public long get() {
            return atomicLong.get();
        }
    }

    private final SerializationHelper<TestContainer> helper = new SerializationHelper<TestContainer>() {
        @Override
        public TestContainer decode(String string) {
            return new TestContainer(UUID.fromString(string));
        }

        @Override
        public String encode(TestContainer value) {
            return value.getUuid().toString();
        }
    };

    private final KeyGenerator<String> keyGenerator = new KeyGenerator<String>() {
        @Override
        public String redisKey(String key) {
            return "KEY:" + key;
        }
    };

    @Test
    public void sniffTest() throws ExecutionException {
        final String key1 = "key1";
        final TestContainer tc1 = new TestContainer(UUID.randomUUID());
        final String key2 = "key2";
        final TestContainer tc2 = new TestContainer(UUID.randomUUID());
        final String barfKey = "barf";

        final ImmutableMap<String, TestContainer> mappings = ImmutableMap.of(key1, tc1, key2, tc2);
        final Function<String, TestContainer> loader = new Function<String, TestContainer>() {
            @Nullable
            @Override
            public TestContainer apply(@Nullable String s) {
                if (barfKey.equals(s)) {
                    throw new RuntimeException(barfKey);
                }
                return mappings.get(s);
            }
        };

        final RDBI rdbi = new RDBI(new JedisPool("localhost"));

        final CounterRunnable hits = new CounterRunnable();
        final CounterRunnable misses = new CounterRunnable();
        final CounterRunnable loadSuccess = new CounterRunnable();
        final CounterRunnable loadFailure = new CounterRunnable();

        final ExecutorService es = new ThreadPoolExecutor(0,
                                                          1,
                                                          200L,
                                                          TimeUnit.SECONDS,
                                                          new ArrayBlockingQueue<Runnable>(10));

        final RedisCache<String, TestContainer> cache = new RedisCache<>(keyGenerator,
                             helper,
                             rdbi,
                             loader,
                             "superFancyCache",
                             "prefix",
                             120,
                             0,
                             60,
                             es,
                             hits,
                             misses,
                             loadSuccess,
                             loadFailure);

        cache.invalidateAll(mappings.keySet());

        assertEquals(tc1.getUuid(), cache.get(key1).getUuid());
        assertEquals(1, misses.get());
        assertEquals(0, hits.get());
        assertEquals(1, loadSuccess.get());
        assertEquals(0, loadFailure.get());
        assertEquals(tc1.getUuid(), cache.get(key1).getUuid());
        assertEquals(1, misses.get());
        assertEquals(1, hits.get());
        assertEquals(1, loadSuccess.get());
        assertEquals(0, loadFailure.get());

        assertNull(cache.get("goobagobbafake"));
        assertEquals(2, misses.get());
        assertEquals(1, hits.get());
        assertEquals(1, loadSuccess.get());
        assertEquals(1, loadFailure.get());


        boolean thrown = false;
        try {
            cache.get(barfKey);
        } catch (ExecutionException ex) {
            thrown = true;
            assertEquals(barfKey, ex.getCause().getMessage());
        }
        assertTrue(thrown);
        assertEquals(3, misses.get());
        assertEquals(1, hits.get());
        assertEquals(1, loadSuccess.get());
        assertEquals(2, loadFailure.get());

        thrown = false;
        try {
            cache.getUnchecked(barfKey);
        } catch (Exception ex) {
            thrown = true;
            assertEquals(barfKey, ex.getMessage());
        }
        assertTrue(thrown);
        assertEquals(4, misses.get());
        assertEquals(1, hits.get());
        assertEquals(1, loadSuccess.get());
        assertEquals(3, loadFailure.get());
    }

    @Test
    public void verifyAsyncitude() throws InterruptedException, ExecutionException {
        // it's a word

        final String key1 = "key1";
        final TestContainer tc1 = new TestContainer(UUID.randomUUID());

        final ArrayBlockingQueue<TestContainer> queue = new ArrayBlockingQueue<>(1);
        final Function<String, TestContainer> mrDeadlock = new Function<String, TestContainer>() {
            @Nullable
            @Override
            public TestContainer apply(@Nullable String s) {
                return queue.poll();
            }
        };

        final ExecutorService es = new ThreadPoolExecutor(0,
                                                          1,
                                                          200L,
                                                          TimeUnit.SECONDS,
                                                          new ArrayBlockingQueue<Runnable>(10));

        final RDBI rdbi = new RDBI(new JedisPool("localhost"));

        final CounterRunnable hits = new CounterRunnable();
        final CounterRunnable misses = new CounterRunnable();
        final CounterRunnable loadSuccess = new CounterRunnable();
        final CounterRunnable loadFailure = new CounterRunnable();

        final RedisCache<String, TestContainer> cache = new RedisCache<>(keyGenerator,
                                                                         helper,
                                                                         rdbi,
                                                                         mrDeadlock,
                                                                         "superFancierCache",
                                                                         "prefix",
                                                                         120,
                                                                         0,
                                                                         60,
                                                                         es,
                                                                         hits,
                                                                         misses,
                                                                         loadSuccess,
                                                                         loadFailure);

        cache.invalidateAll(ImmutableList.of(key1));

        // this call would block if not executed asynchronously
        cache.refresh(key1);
        queue.put(tc1);
        assertEquals(tc1.getUuid(), cache.get(key1).getUuid());
        assertEquals(1, misses.get());
        assertEquals(0, hits.get());
        assertEquals(1, loadSuccess.get());
        assertEquals(0, loadFailure.get());
        assertEquals(tc1.getUuid(), cache.get(key1).getUuid());
        assertEquals(1, misses.get());
        assertEquals(1, hits.get());
        assertEquals(1, loadSuccess.get());
        assertEquals(0, loadFailure.get());
    }
}
