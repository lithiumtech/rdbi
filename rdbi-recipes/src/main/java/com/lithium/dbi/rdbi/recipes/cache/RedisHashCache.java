package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.recipes.locking.RedisSemaphoreDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RedisHashCache<KeyType, ValueType> extends AbstractRedisCache<KeyType, ValueType> implements LoadAllCache<KeyType, ValueType> {

    private static final Logger log = LoggerFactory.getLogger(RedisHashCache.class);

    private final Function<KeyType, String> keyTypeToRedisKey;
    private final Function<ValueType, KeyType> valueTypeToKeyType;
    private final SerializationHelper<ValueType> valueTypeSerializationHelper;
    private final SerializationHelper<KeyType> keyTypeSerializationHelper;
    private final Callable<Collection<ValueType>> loadAll;

    private final String cacheKey;
    private final long cacheRefreshThresholdSecs;
    private final int lockTimeoutSecs;
    private final int lockReleaseRetries;
    private final long lockReleaseRetryWaitMillis;

    /**
     * @param keyGenerator - something that will turn your key object into a string redis can use as a key.
     *                     will be prefixed by the cacheKey string.
     * @param keyTypeSerializationHelper - codec to convert keys to and from a string
     * @param valueKeyGenerator - derive the key from a value.
     * @param valueTypeSerializationHelper - a codec to get your value object to and from a string
     * @param rdbi RDBI instance to use
     * @param loader - function to go get your object
     * @param loadAll - function to go get ALL relevant values
     * @param cacheName - name of cache, used in log statements
     * @param cacheKey - prefix of all keys used by this cache in redis
     * @param cacheRefreshThresholdSecs - if the TTL of the key holding your value in redis is less than this value
     *                                  upon access, we'll automatically asynchronously refresh the value.
     *                                  set to 0 to disable.
     * @param lockTimeoutSecs - write locks on a single value in redis will expire after these seconds
     * @param asyncService - ExecutorService to handle async refreshes - async refresh behavior is disabled if absent.
     * @param hitAction - callback for hits. exceptions are entirely swallowed.
     * @param missAction - callback for misses. exceptions are entirely swallowed.
     * @param loadSuccessAction - callback for load successes. exceptions are entirely swallowed.
     * @param loadExceptionAction - callback for load failures. exceptions are entirely swallowed.
     */
    public RedisHashCache(
            Function<KeyType, String> keyGenerator,
            SerializationHelper<KeyType> keyTypeSerializationHelper,
            Function<ValueType, KeyType> valueKeyGenerator,
            SerializationHelper<ValueType> valueTypeSerializationHelper,
            RDBI rdbi,
            Function<KeyType, ValueType> loader,
            Callable<Collection<ValueType>> loadAll,
            String cacheName,
            String cacheKey,
            long cacheRefreshThresholdSecs,
            int lockTimeoutSecs,
            Optional<ExecutorService> asyncService,
            Runnable hitAction,
            Runnable missAction,
            Runnable loadSuccessAction,
            Runnable loadExceptionAction) {
        super(cacheName, loader, rdbi, asyncService, hitAction, missAction, loadSuccessAction, loadExceptionAction);

        this.keyTypeToRedisKey = keyGenerator;
        this.keyTypeSerializationHelper = keyTypeSerializationHelper;

        this.valueTypeToKeyType = valueKeyGenerator;
        this.valueTypeSerializationHelper = valueTypeSerializationHelper;

        this.loadAll = loadAll;
        this.cacheKey = cacheKey;
        this.cacheRefreshThresholdSecs = cacheRefreshThresholdSecs;
        this.lockTimeoutSecs = lockTimeoutSecs;

        lockReleaseRetries = 3;
        lockReleaseRetryWaitMillis = TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS);
    }

    String cacheLockKey() {
        return cacheKey + ":lock";
    }

    String cacheMissingKey() {
        return cacheKey + ":missing";
    }

    String cacheLoadTimeKey() {
        return cacheKey + ":loadtime";
    }

    @Override
    public boolean acquireLock() {
        return rdbi.withHandle(handle ->
               1 == handle.attach(RedisSemaphoreDAO.class)
                          .acquireSemaphore(
                                  cacheLockKey(),
                                  String.valueOf(System.currentTimeMillis()),
                                  lockTimeoutSecs));
    }

    boolean isLocked() {
        return rdbi.withHandle(handle -> handle.jedis().exists(cacheLockKey()));
    }

    private CallbackResult<ValueType> loadDataSynchronously(final KeyType key) {
        return new AsyncLockFreeRefresher<>(this, key).call();
    }

    private void releaseLock(final Jedis jedis) {
        for(int i = 0; i < lockReleaseRetries; i++) {
            try {
                jedis.del(cacheLockKey());
                return;
            } catch (Exception ex) {
                log.warn("{}: exception releasing lock, will retry", getCacheName(), ex);
                try {
                    Thread.sleep(lockReleaseRetryWaitMillis);
                } catch (InterruptedException e) {
                    log.debug("{}, interrupted retrying lock release", getCacheName(), e);
                }
            }
        }
    }

    @Override
    public void releaseLock() {
        rdbi.consumeHandle(handle -> releaseLock(handle.jedis()));
    }

    @Override
    public CallbackResult<ValueType> getCallback(final KeyType key) {
        // Attempt to load cached data for company from redis. Use new RDBI handle.
        ValueType cachedData = getCachedDataNewJedis(key);
        if (cachedData != null) {
            log.debug("{}: Found cached data for {}", cacheName, key);
            // Regardless of expiration, we got some data so return it.
            markHit();
            return new CallbackResult<>(cachedData);
        }

        // Bad news friend: we don't have anything in the redis cache. Load data and wait until we get it.
        markMiss();
        return loadDataSynchronously(key);
    }

    private ValueType getCachedDataNewJedis(final KeyType key) {
        return rdbi.withHandle(handle -> getCachedData(handle.jedis(), key));
    }

    private ValueType getCachedData(Jedis jedis, KeyType key) {
        final String redisKey = itemKey(key);

        final String result = jedis.hget(cacheKey, itemKey(key));
        if (result == null) {
            return null;
        }
        try {
            final ValueType data = valueTypeSerializationHelper.decode(result);
            return data;
        } catch (Exception e) {
            log.error(cacheName + ": unable to deserialize for " + redisKey, e);
            return null;
        }
    }

    protected String itemKey(KeyType key) {
        return keyTypeToRedisKey.apply(key);
    }

    @Override
    public KeyType keyFromValue(ValueType value) {
        return valueTypeToKeyType.apply(value);
    }

    private void cacheDataRaw(Pipeline pipeline, KeyType key, ValueType data) {
        try {
            // Transform typed key -> redis key (field in redis hash), and encode data -> string
            pipeline.hset(cacheKey, keyTypeToRedisKey.apply(key), valueTypeSerializationHelper.encode(data));
            // Encode typed key -> string to remove from missing set.
            pipeline.srem(cacheMissingKey(), keyTypeSerializationHelper.encode(key));
        } catch (Exception jpe) {
            throw new RuntimeException(jpe);
        }
    }

    @VisibleForTesting
    Set<KeyType> getMissing() {
        return rdbi.withHandle(handle -> {
                Set<String> missing = handle.jedis().smembers(cacheMissingKey());
                Set<KeyType> result = new HashSet<>();
                for (String rawMissingKey : missing) {
                    result.add(keyTypeSerializationHelper.decode(rawMissingKey));
                }
                return result;
        });
    }

    @Override
    public Collection<ValueType> fetchAll() throws Exception {
        return loadAll.call();
    }

    private long loadTimestamp() {
        return rdbi.withHandle(handle -> {
                String loadTimeStr = handle.jedis().get(cacheLoadTimeKey());
                if (loadTimeStr != null) {
                    return Long.parseLong(loadTimeStr);
                } else {
                    return 0L;
                }
        });
    }

    @Override
    public void loadAllComplete() {
        rdbi.consumeHandle(handle -> {
                Jedis jedis = handle.jedis();
                jedis.set(cacheLoadTimeKey(), String.valueOf(System.currentTimeMillis()));
                jedis.del(cacheMissingKey());
        });
    }

    public boolean needsRefresh() {
        return System.currentTimeMillis() - loadTimestamp() > (cacheRefreshThresholdSecs * 1000L);
    }

    /**
     * Refresh if and only if {@link #needsRefresh()} returns true. If a refresh is not needed,
     * you will get a canceled future that throws a {@link CancellationException} on {@link Future#get()}
     * @return Future for result from refreshing
     */
    public Future<CallbackResult<Collection<ValueType>>> refreshAll() {
        if (needsRefresh()) {
            AsyncCacheAllRefresher<KeyType, ValueType> asyncCacheAllRefresher = new AsyncCacheAllRefresher<>(this);
            if(asyncService.isPresent()) {
                return asyncService.get().submit(asyncCacheAllRefresher);
            } else {
                CallbackResult<Collection<ValueType>> syncResult = asyncCacheAllRefresher.call();
                return Futures.immediateFuture(syncResult);
            }
        } else {
            return Futures.immediateCancelledFuture();
        }
    }

    @Override
    public void refresh(final KeyType key) {
        AsyncLockFreeRefresher<KeyType, ValueType> refresher = new AsyncLockFreeRefresher<>(this, key);
        if(asyncService.isPresent()) {
            asyncService.get().submit(refresher);
        } else {
            refresher.call();
        }
    }

    @Override
    public ConcurrentMap<KeyType, ValueType> asMap() {
        // Refresh any invalidated keys before processing the data
        cleanUp();
        List<ValueType> typedCachedData = rdbi
                .withHandle(handle -> handle.jedis().hvals(cacheKey))
                .stream()
                .map(valueTypeSerializationHelper::decode)
                .collect(Collectors.toList());
        return new ConcurrentHashMap<>(uniqueIndex(typedCachedData));
    }

    @Nullable
    @Override
    public ValueType getIfPresent(Object objKey) {
        if (objKey == null) {
            log.warn("{}: Null key provided", cacheName);
            return null;
        }
        KeyType key = (KeyType)objKey;
        ValueType cachedData = getCachedDataNewJedis(key);
        if (cachedData != null) {
            log.debug("{}: Found cached data: {}", cacheName, key);
            markHit();
            return cachedData;
        }
        markMiss();
        return null;
    }

    @Override
    public ValueType get(KeyType key, Callable<? extends ValueType> valueLoader) throws ExecutionException {
        ValueType cachedData = getCachedDataNewJedis(key);
        if (cachedData != null) {
            log.debug("{}: Found cached data: {}", cacheName, key);
            markHit();
            return cachedData;
        }

        markMiss();
        long start = System.currentTimeMillis();
        try {
            ValueType result = valueLoader.call();
            put(key, result);
            markLoadSuccess(System.currentTimeMillis() - start);
            return result;
        } catch(Exception e) {
            log.error("{}: Failed to load for {}", cacheName, itemKey(key), e);
            markLoadException(System.currentTimeMillis() - start);
            throw new ExecutionException(e);
        }
    }

    @Override
    public ImmutableMap<KeyType, ValueType> getAll(Iterable<? extends KeyType> keys) throws ExecutionException {
        // Cleanup should load any missing keys.
        cleanUp();
        return getAllPresent(keys);
    }

    @Override
    public ImmutableMap<KeyType, ValueType> getAllPresent(final Iterable<?> keys) {
        return rdbi.withHandle(handle -> {
                /*
                 * The basic operation works like this:
                 *
                 * 1. Assume all keys are of type KeyType
                 * 2. Convert the Iterable into:
                 *   a. List<KeyType> - will be used to construct the Map at the end.
                 *   b. List<String> - will be used to bulk get items from our redis hash
                 * 3. Bulk get from redis using the List<String>
                 * 4. Iterate over redis results, ignoring null values (missing
                 *    from cache), and deserializing strings into ValueType.
                 *    Pair each ValueType with the KeyType at the same index
                 *    and add the pair to the result map builder.
                 * 5. Return map
                 */
                Jedis jedis = handle.jedis();
                // Prevent duplicate entries using a HashSet
                Set<KeyType> alreadyAdded = new HashSet<>();
                List<KeyType> typedKeys = new ArrayList<>();
                List<String> redisItemKeys = new ArrayList<>();
                for (Object objKey : keys) {
                    if (alreadyAdded.contains(objKey)) {
                        continue;
                    }
                    KeyType key = (KeyType) objKey;
                    alreadyAdded.add(key);
                    typedKeys.add(key);
                    redisItemKeys.add(itemKey(key));
                }
                // The bulk operation HMGET takes a variadic argument for the
                // list of keys to multi-get. The easiest way for us to call
                // this to convert our list to String[] and pass that.
                String[] redisItemKeysArray = new String[redisItemKeys.size()];
                redisItemKeys.toArray(redisItemKeysArray);
                List<String> rawValues = jedis.hmget(cacheKey, redisItemKeysArray);

                ImmutableMap.Builder<KeyType, ValueType> mapBuilder = ImmutableMap.builder();
                for (int i = 0; i < rawValues.size(); i++) {
                    String rawValue = rawValues.get(i);
                    if (rawValue == null) {
                        continue;
                    }
                    ValueType typedValue = valueTypeSerializationHelper.decode(rawValue);
                    mapBuilder.put(typedKeys.get(i), typedValue);
                }
                return mapBuilder.build();
        });
    }

    @Override
    public void put(final KeyType key, final ValueType value) {
        rdbi.consumeHandle(handle -> {
                Pipeline pipeline = handle.jedis().pipelined();
                cacheDataRaw(pipeline, key, value);
                pipeline.sync();
        });
    }

    @Override
    public void putAll(final Map<? extends KeyType, ? extends ValueType> data) {
        rdbi.consumeHandle(handle -> {
                Pipeline pipeline = handle.jedis().pipelined();
                putAllInternal(data, pipeline);
                pipeline.sync();
        });
    }

    private void putAllInternal(final Map<? extends KeyType, ? extends ValueType> data, Pipeline pipeline) {
        for (Map.Entry<? extends KeyType, ? extends ValueType> entry : data.entrySet()) {
            cacheDataRaw(pipeline, entry.getKey(), entry.getValue());
        }
    }

    /**
     * Signals that a (possibly) cached item no longer exists.
     * @see #invalidate(Object)
     * @param key key to remove from hash
     */
    public void remove(final KeyType key) {
        if (key == null) {
            log.warn("{}: Remove requested for null key", cacheName);
            return;
        }
        rdbi.consumeHandle(handle -> {
                Pipeline pipeline = handle.jedis().pipelined();
                // remove the cached value (if any)
                pipeline.hdel(cacheKey, itemKey(key));
                // clear out missing cache key (if any)
                pipeline.srem(cacheMissingKey(), keyTypeSerializationHelper.encode(key));
                pipeline.sync();
        });
    }

    private Map<KeyType, ValueType> uniqueIndex(Collection<ValueType> values) {
        return values.stream()
                     .filter(value -> {
                         if (value == null) {
                             log.warn("{}: Omitting a null value from the result set.", cacheName);
                         }
                         return value != null;
                     })
                     .collect(Collectors.toMap(this::keyFromValue,
                                               Function.identity(),
                                               (a, b) -> a));
    }

    @Override
    public void applyFetchAll(Collection<ValueType> fetched) {
        final Map<KeyType, ValueType> fetchedAndIndexed = uniqueIndex(fetched);

        rdbi.consumeHandle(handle -> {
                Pipeline pipeline = handle.jedis().pipelined();
                removeAllCachedData(pipeline);
                putAllInternal(fetchedAndIndexed, pipeline);
                pipeline.sync();
        });
    }

    /**
     * Removes the cached value for the key, but retains the key for future refresh query.
     * @see #remove(Object)
     * @param objKey key to mark as invalidated
     */
    @Override
    public void invalidate(Object objKey) {
        if (objKey == null) {
            log.warn("{}: Invalidation requested for null key", cacheName);
            return;
        }
        final KeyType key = (KeyType) objKey;
        rdbi.consumeHandle(handle -> {
                Pipeline pipeline = handle.jedis().pipelined();
                invalidate(pipeline, key);
                pipeline.sync();
        });
        cacheEvictionCount.incrementAndGet();
    }

    private void invalidate(Pipeline pipeline, KeyType key) {
        String itemKey = itemKey(key);
        pipeline.hdel(cacheKey, itemKey);
        pipeline.sadd(cacheMissingKey(), keyTypeSerializationHelper.encode(key));
    }

    @Override
    public void invalidateAll(final Iterable<?> keys) {
        rdbi.consumeHandle(handle -> {
                Pipeline pipeline = handle.jedis().pipelined();
                for (final Object objKey : keys) {
                    invalidate(pipeline, (KeyType) objKey);
                }
                pipeline.sync();
        });
    }

    @Override
    public void invalidateAll() {
        rdbi.consumeHandle(handle -> {
                Pipeline pipeline = handle.jedis().pipelined();
                removeAllCachedData(pipeline);
                pipeline.del(cacheLockKey());
                pipeline.del(cacheLoadTimeKey());
                pipeline.sync();
        });
    }

    /**
     * An internal operation that removes only true data points from the cache.
     * Meta-data for locking and refresh timing are not altered.
     * @param pipeline jedis pipeline
     * @see #invalidateAll() for the public analog that also alters meta-data
     */
    private void removeAllCachedData(Pipeline pipeline) {
        pipeline.del(cacheKey);
        pipeline.del(cacheMissingKey());
    }

    @Override
    public long size() {
        return rdbi.withHandle(handle -> {
                Jedis jedis = handle.jedis();
                long cachedValues = jedis.hlen(cacheKey);
                long missingValues = jedis.scard(cacheMissingKey());
                return cachedValues + missingValues;
        });
    }

    @Override
    public void cleanUp() {
        Set<KeyType> missing = getMissing();
        for (KeyType key : missing) {
            refresh(key);
        }
    }
}
