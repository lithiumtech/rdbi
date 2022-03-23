package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(groups = "integration")
public class RedisHashCacheTest {

    private static final String TEST_NAMESPACE = RedisHashCacheTest.class.getSimpleName();

    private ExecutorService es;

    @Test
    public void sniffTest() throws ExecutionException {
        final String key1 = "key1";
        final TestContainer tc1 = new TestContainer(key1, UUID.randomUUID(), "sniff-1");
        final String key2 = "key2";
        final TestContainer tc2 = new TestContainer(key2, UUID.randomUUID(), "sniff-2");
        final String barfKey = "barf";

        final ImmutableMap<String, TestContainer> mappings = ImmutableMap.of(key1, tc1, key2, tc2);
        final Function<String, TestContainer> loader = s -> {
                if (barfKey.equals(s)) {
                    throw new RuntimeException(barfKey);
                }
                return mappings.get(s);
        };

        final Callable<Collection<TestContainer>> loadAll = mappings::values;

        final CounterRunnable hits = new CounterRunnable();
        final CounterRunnable misses = new CounterRunnable();
        final CounterRunnable loadSuccess = new CounterRunnable();
        final CounterRunnable loadFailure = new CounterRunnable();

        final RedisHashCache<String, TestContainer> cache =
                new RedisHashCache<>(
                        keyGenerator,
                        keyCodec,
                        valueGenerator,
                        helper,
                        createRdbi(),
                        loader,
                        loadAll,
                        "superFancyCache",
                        TEST_NAMESPACE,
                        120,
                        120,
                        0,
                        Optional.of(es),
                        hits,
                        misses,
                        loadSuccess,
                        loadFailure);

        // Clear out any potential preexisting data.
        cache.invalidateAll();

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
            assertEquals(barfKey, ex.getCause().getCause().getMessage());
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
            assertEquals(barfKey, ex.getCause().getMessage());
        }
        assertTrue(thrown);
        assertEquals(4, misses.get());
        assertEquals(1, hits.get());
        assertEquals(1, loadSuccess.get());
        assertEquals(3, loadFailure.get());
    }

    @Test
    public void sniffTest2() throws ExecutionException, InterruptedException {
        final String key1 = "key1";
        final TestContainer tc1 = new TestContainer(key1, UUID.randomUUID(), "sniff2-1");

        final String key2 = "key2";
        final TestContainer tc2 = new TestContainer(key2, UUID.randomUUID(), "sniff2-2");

        final String key3 = "key3";
        final TestContainer tc3 = new TestContainer(key3, UUID.randomUUID(), "sniff2-3");

        final Map<String, TestContainer> dataSource = ImmutableMap.of(key1, tc1, key2, tc2, key3, tc3);

        final RedisHashCache<String, TestContainer> cache =
                new RedisHashCache<>(
                        keyGenerator,
                        keyCodec,
                        valueGenerator,
                        helper,
                        createRdbi(),
                        fetchFromMap(dataSource),
                        fetchAllFromMap(dataSource),
                        "superFancyCache",
                        TEST_NAMESPACE,
                        120,
                        120,
                        0,
                        Optional.<ExecutorService>empty(), // Force synchronous calls!
                        NOOP,
                        NOOP,
                        NOOP,
                        NOOP);

        // Clear out any potential preexisting data.
        cache.invalidateAll();

        assertEquals(0, cache.size());
        Collection<TestContainer> refreshedResults = cache.refreshAll().get().getOrThrowUnchecked();
        assertEquals(3, refreshedResults.size());

        assertEquals(3, cache.size());

        cache.invalidate(key1);
        assertEquals(ImmutableSet.of(key1), cache.getMissing());

        // key1/tc1 should no longer be in the cache and getAllPresent does not produce it
        Map<String, TestContainer> dataInRedis = cache.getAllPresent(ImmutableList.of(key1, key2, key3));
        assertEquals(2, dataInRedis.size());
        assertFalse(dataInRedis.containsKey(key1));
        assertTrue(dataInRedis.containsKey(key2));
        assertTrue(dataInRedis.containsKey(key3));
        // Size should include the "missing" data for key1
        assertEquals(3, cache.size());
        // Getting key1 should load the value from the data source
        assertEquals(tc1, cache.get(key1));
        // The missing set should have been cleared out
        assertEquals(Collections.emptySet(), cache.getMissing());

        // Invalidate key2
        cache.invalidate(key2);
        // Missing should contain key2
        assertEquals(ImmutableSet.of(key2), cache.getMissing());
        // key1/tc1 should no longer be in the cache but getAll should produce it
        Map<String, TestContainer> dataInRedis2 = cache.getAll(ImmutableList.of(key1, key2, key3));
        assertEquals(3, dataInRedis2.size());
        assertTrue(dataInRedis2.containsKey(key1));
        assertTrue(dataInRedis2.containsKey(key2));
        assertTrue(dataInRedis2.containsKey(key3));
        // Missing should now be empty
        assertEquals(Collections.emptySet(), cache.getMissing());

        // Re-invalidate key3
        cache.invalidate(key3);
        // Missing should contain key3
        assertEquals(ImmutableSet.of(key3), cache.getMissing());
        // key1/tc1 should no longer be in the cache but asMap should produce it
        Map<String, TestContainer> dataInRedis3 = cache.asMap();
        assertEquals(3, dataInRedis3.size());
        assertTrue(dataInRedis3.containsKey(key1));
        assertTrue(dataInRedis3.containsKey(key2));
        assertTrue(dataInRedis3.containsKey(key3));
        // Missing should now be empty
        assertEquals(Collections.emptySet(), cache.getMissing());
    }

    @Test
    public void getAllPresentWorksWithDuplicateKeys() {
        final String key1 = "key1";
        final TestContainer tc1 = new TestContainer(key1, UUID.randomUUID(), "with-dupes-1");

        final String key2 = "key2";
        final TestContainer tc2 = new TestContainer(key2, UUID.randomUUID(), "with-dupes-2");

        final String key3 = "key3";
        final TestContainer tc3 = new TestContainer(key3, UUID.randomUUID(), "with-dupes-3");

        final Map<String, TestContainer> dataSource = ImmutableMap.of(key1, tc1, key2, tc2, key3, tc3);

        final Function<String, TestContainer> loader = dataSource::get;

        final Callable<Collection<TestContainer>> loadAll = dataSource::values;

        final RedisHashCache<String, TestContainer> cache =
                new RedisHashCache<>(
                        keyGenerator,
                        keyCodec,
                        valueGenerator,
                        helper,
                        createRdbi(),
                        loader,
                        loadAll,
                        "superFancyCache",
                        TEST_NAMESPACE,
                        120,
                        120,
                        0,
                        Optional.<ExecutorService>empty(), // Force synchronous calls!
                        NOOP,
                        NOOP,
                        NOOP,
                        NOOP);

        // Clear out any potential preexisting data.
        cache.invalidateAll();

        assertEquals(0, cache.size());
        cache.refreshAll();

        Map<String, TestContainer> results = cache.getAllPresent(ImmutableList.of(key1, key1, key2, key3));
        assertTrue(results.containsKey(key1));
        assertEquals(tc1, results.get(key1));

        assertTrue(results.containsKey(key2));
        assertEquals(tc2, results.get(key2));

        assertTrue(results.containsKey(key3));
        assertEquals(tc3, results.get(key3));
    }

    @Test
    public void verifyAsyncitude() throws InterruptedException, ExecutionException {
        // it's a word

        final String key1 = "key1";
        final TestContainer tc1 = new TestContainer(key1, UUID.randomUUID(), "async-1");

        final ArrayBlockingQueue<TestContainer> queue = new ArrayBlockingQueue<>(1);
        final Function<String, TestContainer> mrDeadlock = new Function<String, TestContainer>() {
            @Nullable
            @Override
            public TestContainer apply(@Nullable String s) {
                try {
                    return queue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        final Callable<Collection<TestContainer>> loadAll = Collections::emptyList;

        final CounterRunnable hits = new CounterRunnable();
        final CounterRunnable misses = new CounterRunnable();
        final CounterRunnable loadSuccess = new CounterRunnable();
        final CounterRunnable loadFailure = new CounterRunnable();

        final RedisHashCache<String, TestContainer> cache =
                new RedisHashCache<>(
                        keyGenerator,
                        new PassthroughSerializationHelper(),
                        valueGenerator,
                        helper,
                        createRdbi(),
                        mrDeadlock,
                        loadAll,
                        "superFancyCache",
                        TEST_NAMESPACE,
                        120,
                        120,
                        0,
                        Optional.of(es),
                        hits,
                        misses,
                        loadSuccess,
                        loadFailure);

        // Clear out any potential preexisting data.
        cache.invalidateAll();

        // this call would block if not executed asynchronously
        cache.refresh(key1);
        queue.put(tc1);
        while (queue.size() > 0 || cache.isLocked()) {
            Thread.sleep(50);
        }
        assertEquals(tc1.getUuid(), cache.get(key1).getUuid());
        assertEquals(0, misses.get());
        assertEquals(1, hits.get());
        assertEquals(1, loadSuccess.get());
        assertEquals(0, loadFailure.get());
        assertEquals(tc1.getUuid(), cache.get(key1).getUuid());
        assertEquals(0, misses.get());
        assertEquals(2, hits.get());
        assertEquals(1, loadSuccess.get());
        assertEquals(0, loadFailure.get());
    }

    @Test
    public void testRemove() throws ExecutionException {

        final String key1 = "key1";
        final TestContainer originalValueForKey1 = new TestContainer(key1, UUID.randomUUID(), "test-remove-1");

        final String key2 = "key2";
        final TestContainer originalValueForKey2 = new TestContainer(key2, UUID.randomUUID(), "test-remove-2");

        final Map<String, TestContainer> dataSource = Maps.newHashMap();
        dataSource.put(key1, originalValueForKey1);
        dataSource.put(key2, originalValueForKey2);

        final RDBI rdbi = createRdbi();

        final RedisHashCache<String, TestContainer> cache =
                new RedisHashCache<>(
                        keyGenerator,
                        new PassthroughSerializationHelper(),
                        valueGenerator,
                        helper,
                        rdbi,
                        fetchFromMap(dataSource),
                        fetchAllFromMap(dataSource),
                        "testRemoveCache",
                        TEST_NAMESPACE,
                        120,
                        0,
                        0,
                        Optional.of(es),
                        NOOP,
                        NOOP,
                        NOOP,
                        NOOP);

        // invalidate (for any prior test run)
        cache.invalidateAll();

        // cache contains expected keys
        assertEquals(originalValueForKey1, cache.get(key1));
        assertEquals(originalValueForKey2, cache.get(key2));

        // manipulate the data source for key1
        final TestContainer newValueForKey1 = new TestContainer(key1, UUID.randomUUID(), "test-remove-new-value-for-key-1");
        dataSource.put(key1, newValueForKey1);
        assertEquals(originalValueForKey1, cache.get(key1));

        // invalidate the key ... should reload on next request
        cache.invalidate(key1);
        assertTrue(cache.getMissing().contains(key1));
        assertEquals(newValueForKey1, cache.get(key1));
        assertEquals(originalValueForKey2, cache.get(key2));
        assertTrue(cache.getMissing().isEmpty());

        // manipulate the data source for key1
        dataSource.remove(key1);
        assertEquals(newValueForKey1, cache.get(key1));

        // remove the key from cache
        assertEquals(2, cache.size());
        cache.remove(key1);
        assertTrue(cache.getMissing().isEmpty());
        assertEquals(null, cache.get(key1));
        assertEquals(originalValueForKey2, cache.get(key2));
        assertEquals(1, cache.size());

        // manipulate the data source for key2
        dataSource.remove(key2);
        assertEquals(originalValueForKey2, cache.get(key2));

        // remove a previously invalidated key from cache
        assertEquals(1, cache.size());
        cache.invalidate(key2);
        assertTrue(cache.getMissing().contains(key2));
        cache.remove(key2);
        assertTrue(cache.getMissing().isEmpty());
        assertEquals(null, cache.get(key1));
        assertEquals(null, cache.get(key2));

        // remove a key that doesn't exist
        cache.remove("not_a_legit_key");
    }

    @Test
    public void testInvalidateAll() throws ExecutionException {

        final String key1 = "key1";
        final TestContainer originalValueForKey1 = new TestContainer(key1, UUID.randomUUID(), "invalidate-1");

        final String key2 = "key2";
        final TestContainer originalValueForKey2 = new TestContainer(key2, UUID.randomUUID(), "invalidate-2");

        final Map<String, TestContainer> dataSource = Maps.newHashMap();
        dataSource.put(key1, originalValueForKey1);
        dataSource.put(key2, originalValueForKey2);

        final RedisHashCache<String, TestContainer> cache =
                new RedisHashCache<>(
                        keyGenerator,
                        new PassthroughSerializationHelper(),
                        valueGenerator,
                        helper,
                        createRdbi(),
                        fetchFromMap(dataSource),
                        fetchAllFromMap(dataSource),
                        "testInvalidateAllCache",
                        TEST_NAMESPACE,
                        120,
                        120,
                        0,
                        Optional.of(es),
                        NOOP,
                        NOOP,
                        NOOP,
                        NOOP);

        // invalidate (for any prior test run)
        cache.invalidateAll();

        // cache contains expected keys
        assertEquals(originalValueForKey1, cache.get(key1));
        assertEquals(originalValueForKey2, cache.get(key2));

        // manipulate the data source
        dataSource.clear();

        // remove all cache entries
        cache.invalidateAll();
        assertNull(cache.get(key1));
        assertNull(cache.get(key2));

        // subsequent remove all call should not explode
        cache.invalidateAll();

        // manipulate the data source
        final TestContainer newValueForKey1 = new TestContainer(key1, UUID.randomUUID(), "invalidate-3");
        dataSource.put(key1, newValueForKey1);
        assertEquals(newValueForKey1, cache.get(key1));
    }

    @Test
    public void testSyncWithDataSource() throws ExecutionException, InterruptedException {

        final String key1 = "key1";
        final TestContainer key1Value = new TestContainer(key1, UUID.randomUUID(), "sync-1");

        final String key2 = "key2";
        final TestContainer key2Value = new TestContainer(key2, UUID.randomUUID(), "sync-2");

        final Map<String, TestContainer> dataSource = Maps.newHashMap();
        dataSource.put(key1, key1Value);
        dataSource.put(key2, key2Value);

        final RDBI rdbi = createRdbi();

        final RedisHashCache<String, TestContainer> cache =
                new RedisHashCache<>(
                        keyGenerator,
                        new PassthroughSerializationHelper(),
                        valueGenerator,
                        helper,
                        rdbi,
                        fetchFromMap(dataSource),
                        fetchAllFromMap(dataSource),
                        "testNullValueCache",
                        TEST_NAMESPACE,
                        120,
                        120,
                        0,
                        Optional.of(es),
                        NOOP,
                        NOOP,
                        NOOP,
                        NOOP);

        // invalidate (for any prior test run)
        cache.invalidateAll();

        // cache contains expected keys
        assertEquals(key1Value, cache.get(key1));
        assertEquals(key2Value, cache.get(key2));
        assertEquals(2, cache.asMap().size());

        // update the data source and invalidate cache
        dataSource.remove(key1);
        cache.invalidateAll();

        // cache contains expected keys
        assertNull(cache.get(key1));
        assertEquals(key2Value, cache.get(key2));
        assertEquals(1, cache.asMap().size());
    }

    @Test
    public void testNullRefreshDiscarded() throws ExecutionException {

        final String key = "key";
        final TestContainer value = new TestContainer(key, UUID.randomUUID(), "null-discarded");

        final Map<String, TestContainer> dataSource = Maps.newHashMap();
        dataSource.put(key, value);

        final RedisHashCache<String, TestContainer> cache =
                new RedisHashCache<>(
                        keyGenerator,
                        new PassthroughSerializationHelper(),
                        valueGenerator,
                        helper,
                        createRdbi(),
                        fetchFromMap(dataSource),
                        fetchAllFromMap(dataSource),
                        "testNullRefreshDiscarded",
                        TEST_NAMESPACE,
                        120,
                        120,
                        0,
                        Optional.of(es),
                        NOOP,
                        NOOP,
                        NOOP,
                        NOOP);

        // invalidate (for any prior test run)
        cache.invalidateAll();

        // cache contains expected key
        assertEquals(value, cache.get(key));
        assertEquals(1, cache.asMap().size());

        // update the data source and invalidate cache
        dataSource.put(key, null);
        cache.invalidateAll();

        // cache reflects new null value
        assertNull(cache.get(key));
        assertTrue(cache.asMap().isEmpty());

        // hard refresh the key
        cache.refresh(key);
        assertNull(cache.get(key));
        assertTrue(cache.asMap().isEmpty());

        // hard refresh the entire cache
        cache.refreshAll();
        assertNull(cache.get(key));
        assertTrue(cache.asMap().isEmpty());
    }

    @Test
    public void testFetchExceptionDoesNotBurnCache() throws ExecutionException, InterruptedException {

        final String key = "key";
        final TestContainer value = new TestContainer(key, UUID.randomUUID(), "fetch-not-burn-1");

        final Map<String, TestContainer> dataSource = Maps.newHashMap();
        dataSource.put(key, value);

        final Function<String, TestContainer> fetchOnceFromMap = new Function<String, TestContainer>() {
            private AtomicBoolean fetched = new AtomicBoolean(false);

            @Override
            public TestContainer apply(@Nullable String s) {
                if (fetched.getAndSet(true)) {
                    throw new RuntimeException("oh-noes");
                }

                return dataSource.get(s);
            }
        };


        final Callable<Collection<TestContainer>> fetchAllAlwaysFails = () -> {
            throw new RuntimeException("oh-noes");
        };

        final RedisHashCache<String, TestContainer> cache =
                new RedisHashCache<>(
                        keyGenerator,
                        new PassthroughSerializationHelper(),
                        valueGenerator,
                        helper,
                        createRdbi(),
                        fetchOnceFromMap,
                        fetchAllAlwaysFails,
                        "testFetchExceptionDoesNotBurnCache",
                        TEST_NAMESPACE,
                        120,
                        Integer.MAX_VALUE,
                        0,
                        Optional.<ExecutorService>empty(),
                        NOOP,
                        NOOP,
                        NOOP,
                        NOOP);

        // invalidate (for any prior test run)
        cache.invalidateAll();

        // spend our one good refresh() operation
        cache.refresh(key);

        // cache contains expected key
        assertEquals(value, cache.get(key));
        assertEquals(value, cache.asMap().get(key));
        assertEquals(1, cache.asMap().size());

        // update the data source and refresh cache
        dataSource.put(key, new TestContainer(key, UUID.randomUUID(), "fetch-not-burn-2"));
        cache.refreshAll();
        cache.refresh(key);

        // old value is still cached
        assertEquals(value, cache.asMap().get(key));
        assertEquals(1, cache.asMap().size());
    }

    @Test
    public void testFetchNullResultDoesNotBurnCache() throws ExecutionException, InterruptedException {

        final String key = "key";
        final TestContainer value = new TestContainer(key, UUID.randomUUID(), "not-burn-1");

        final Map<String, TestContainer> dataSource = Maps.newHashMap();
        dataSource.put(key, value);

        final Function<String, TestContainer> fetchOnceFromMap = new Function<String, TestContainer>() {
            private AtomicBoolean fetched = new AtomicBoolean(false);

            @Override
            public TestContainer apply(@Nullable String s) {
                if (fetched.getAndSet(true)) {
                    return null;
                }

                return dataSource.get(s);
            }
        };

        final Callable<Collection<TestContainer>> fetchAllAlwaysFails = () -> null;

        final RedisHashCache<String, TestContainer> cache =
                new RedisHashCache<>(
                        keyGenerator,
                        new PassthroughSerializationHelper(),
                        valueGenerator,
                        helper,
                        createRdbi(),
                        fetchOnceFromMap,
                        fetchAllAlwaysFails,
                        "testFetchNullResultDoesNotBurnCache",
                        TEST_NAMESPACE,
                        120,
                        120,
                        0,
                        Optional.<ExecutorService>empty(),
                        NOOP,
                        NOOP,
                        NOOP,
                        NOOP);

        // invalidate (for any prior test run)
        cache.invalidateAll();

        // spend our one good refresh() operation
        cache.refresh(key);

        // cache contains expected key
        assertEquals(value, cache.get(key));
        assertEquals(value, cache.asMap().get(key));
        assertEquals(1, cache.asMap().size());

        // update the data source and refresh cache
        dataSource.put(key, new TestContainer(key, UUID.randomUUID(), "not-burn-2"));
        cache.refreshAll();
        cache.refresh(key);

        // old value is still cached
        assertEquals(value, cache.get(key));
        assertEquals(1, cache.asMap().size());
    }

    @Test
    public void testNeedsRefresh() throws InterruptedException, ExecutionException, TimeoutException {

        final String key = "key";
        final TestContainer value = new TestContainer(key, UUID.randomUUID(), "needs-refresh-1");

        final Map<String, TestContainer> dataSource = Maps.newHashMap();
        dataSource.put(key, value);

        final RedisHashCache<String, TestContainer> cache =
                new RedisHashCache<>(
                        keyGenerator,
                        new PassthroughSerializationHelper(),
                        valueGenerator,
                        helper,
                        createRdbi(),
                        fetchFromMap(dataSource),
                        fetchAllFromMap(dataSource),
                        "testNeedsRefresh",
                        TEST_NAMESPACE,
                        120,
                        Duration.ofMinutes(10).toMillis(),
                        0,
                        Optional.of(es),
                        NOOP,
                        NOOP,
                        NOOP,
                        NOOP);

        // after refresh all, should not need to refresh again
        Future<CallbackResult<Collection<TestContainer>>> f = cache.refreshAll();
        if (!f.isCancelled()) {
            f.get(500, TimeUnit.MILLISECONDS);
        }
        assertFalse(cache.needsRefresh());

        // invalidating a single key doesn't mark the entire cache as stale
        cache.invalidate(key);
        assertFalse(cache.needsRefresh());

        // but now invalidate the cache at large and it should be considered stale
        cache.invalidateAll();
        assertTrue(cache.needsRefresh());
    }

    RDBI createRdbi() {
        return new RDBI(new JedisPool("localhost", 6379));
    }

    public static class TestContainer {

        private final String key;
        private final UUID uuid;
        private final String label;

        public TestContainer(String key, UUID uuid, String label) {
            this.key = key;
            this.uuid = uuid;
            this.label = label;
        }

        public String getKey() {
            return key;
        }

        public UUID getUuid() {
            return uuid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestContainer that = (TestContainer) o;
            return Objects.equals(key, that.key) &&
                    Objects.equals(uuid, that.uuid) &&
                    Objects.equals(label, that.label);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, uuid, label);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("TestContainer{");
            sb.append("key='").append(key).append('\'');
            sb.append(", uuid=").append(uuid);
            sb.append(", label='").append(label).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    public static final Runnable NOOP = () -> {};

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

    public static final SerializationHelper<TestContainer> helper = new SerializationHelper<TestContainer>() {
        @Override
        public TestContainer decode(String string) {
            String[] data = string.split(",");
            return new TestContainer(data[0], UUID.fromString(data[1]), data[2]);
        }

        @Override
        public String encode(TestContainer value) {
            return value.getKey() + "," + value.getUuid().toString() + "," + value.label;
        }
    };

    public static final SerializationHelper<String> keyCodec = new SerializationHelper<String>() {
        @Override
        public String decode(String string) {
            return string.substring("encoded-key:".length());
        }

        @Override
        public String encode(String value) {
            return "encoded-key:" + value;
        }
    };

    public static final Function<String, String> keyGenerator = key -> "item-key:" + key;

    public static final Function<TestContainer, String> valueGenerator = TestContainer::getKey;

    private <T> Function<String, T> fetchFromMap(final Map<String, T> dataSource) {
        return dataSource::get;
    }

    private <T> Callable<Collection<T>> fetchAllFromMap(final Map<String, T> dataSource) {
        return dataSource::values;
    }

    @BeforeMethod
    public void setupExecutor() {
        this.es = Executors.newSingleThreadExecutor();
    }

    @BeforeMethod
    public void clearRedis() {
        createRdbi().withHandle((Callback<Void>) handle -> {
            handle.jedis().del(TEST_NAMESPACE);
            return null;
        });
    }

    @AfterMethod
    public void shutdownExecutor() {
        es.shutdownNow();
    }
}
