package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class RedisCache<KeyType, ValueType> implements LoadingCache<KeyType, ValueType> {
    private static final Logger log = LoggerFactory.getLogger(RDBI.class);
    private final KeyGenerator<KeyType> redisKeyGenerator;
    private final SerializationHelper<ValueType> serializationHelper;
    private final Function<KeyType, ValueType> loader;
    private final RDBI rdbi;
    private final String cacheName;
    private final String keyPrefix;
    private final int valueTtl;
    private final long cacheRefreshThresholdSecs;
    private final int lockTimeout;
    private final Runnable hitAction;
    private final Runnable missAction;
    private final Runnable loadSuccessAction;
    private final Runnable loadExceptionAction;
    private final ExecutorService asyncService;
    private AtomicLong cacheEvictionCount;
    private AtomicLong cacheHitCount;
    private AtomicLong cacheLoadExceptionCount;
    private AtomicLong cacheLoadSuccessCount;
    private AtomicLong cacheMissCount;
    private AtomicLong cacheTotalLoadTime;

    public RedisCache(KeyGenerator<KeyType> keyGenerator,
                      SerializationHelper<ValueType> serializationHelper,
                      RDBI rdbi,
                      Function<KeyType, ValueType> loader,
                      String cacheName,
                      String keyPrefix,
                      int valueTtlSecs,
                      long cacheRefreshThresholdSecs,
                      int lockTimeout,
                      ExecutorService asyncService,
                      Runnable hitAction,
                      Runnable missAction,
                      Runnable loadSuccessAction,
                      Runnable loadExceptionAction) {
        this.redisKeyGenerator = keyGenerator;
        this.serializationHelper = serializationHelper;
        this.loader = loader;
        this.rdbi = rdbi;
        this.cacheName = cacheName;
        this.keyPrefix = keyPrefix;
        this.valueTtl = valueTtlSecs;
        this.cacheRefreshThresholdSecs = cacheRefreshThresholdSecs;
        this.lockTimeout = lockTimeout;
        this.hitAction = hitAction;
        this.missAction = missAction;
        this.loadSuccessAction = loadSuccessAction;
        this.loadExceptionAction = loadExceptionAction;
        this.asyncService = asyncService;

        this.cacheHitCount = new AtomicLong();
        this.cacheMissCount = new AtomicLong();
        this.cacheLoadSuccessCount = new AtomicLong();
        this.cacheLoadExceptionCount = new AtomicLong();
        this.cacheTotalLoadTime = new AtomicLong();
        this.cacheEvictionCount = new AtomicLong();
    }

    protected void markHit() {
        cacheHitCount.incrementAndGet();
        hitAction.run();
    }

    protected void markMiss() {
        cacheMissCount.incrementAndGet();
        missAction.run();
    }

    protected void markLoadException(final long loadTime) {
        cacheLoadExceptionCount.incrementAndGet();
        cacheTotalLoadTime.addAndGet(loadTime);
        loadExceptionAction.run();
    }

    protected void markLoadSuccess(final long loadTime) {
        cacheLoadSuccessCount.incrementAndGet();
        cacheTotalLoadTime.addAndGet(loadTime);
        loadSuccessAction.run();
    }

    public String getCacheName() {
        return cacheName;
    }

    String redisLockKey(String redisKey) {
        return redisKey + ":lock";
    }

    boolean acquireLock(Jedis jedis, String lockKey) {
        String lockResponse = jedis.set(
                lockKey,
                String.valueOf(System.currentTimeMillis()),
                "NX", // Don't overwrite
                "EX", // Expire after 60 seconds
                lockTimeout);
        return Objects.equal(lockResponse, "OK");
    }

    protected boolean acquireLock(final KeyType key) {
        return rdbi.withHandle(new Callback<Boolean>() {
            @Override
            public Boolean run(Handle handle) {
                return acquireLock(handle.jedis(), redisLockKey(generateRedisKey(key)));
            }
        });
    }

    boolean isLocked(KeyType key) {
        final String redisKey = generateRedisKey(key);
        final String redisLockKey = redisLockKey(redisKey);
        return rdbi.withHandle(new Callback<Boolean>() {
            @Override
            public Boolean run(Handle handle) {
                return handle.jedis().exists(redisLockKey);
            }
        });
    }

    CallbackResult<ValueType> loadDataSynchronously(final KeyType key) {
        return new AsyncCacheRefresher<>(this, rdbi, key).call();
    }

    private void releaseLock(final KeyType key, final Jedis jedis) {
        jedis.del(redisLockKey(generateRedisKey(key)));
    }

    protected void releaseLock(final KeyType key) {
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                releaseLock(key, handle.jedis());
                return null;
            }
        });
    }

    CachedData<ValueType> getCachedDataNewJedis(final KeyType key) {
        return rdbi.withHandle(new Callback<CachedData<ValueType>>() {
            @Override
            public CachedData<ValueType> run(Handle handle) {
                return getCachedData(handle.jedis(), key);
            }
        });
    }

    CachedData<ValueType> getCachedData(Jedis jedis, KeyType key) {
        final String redisKey = generateRedisKey(key);

        final String result = jedis.get(redisKey);
        if (result == null) {
            return null;
        }
        final Long secondsRemaining = jedis.ttl(redisKey);
        try {
            final ValueType data = serializationHelper.decode(result);
            return new CachedData<>(secondsRemaining, data);
        } catch (Exception e) {
            log.error(cacheName + ": unable to deserialize for " + redisKey, e);
            return null;
        }
    }

    protected String generateRedisKey(KeyType key) {
        return keyPrefix + redisKeyGenerator.redisKey(key);
    }

    @VisibleForTesting
    public String cacheData(KeyType key, Jedis jedis, ValueType data, int ttlSeconds) {
        if (data == null) {
            log.error("{}: Cowardly refusing to cache null data to redis: {}", cacheName, key);
            return null;
        }

        final String redisKey = generateRedisKey(key);
        String response = null;
        try {
            String saveStr = serializationHelper.encode(data);
            response = jedis.setex(redisKey, ttlSeconds, saveStr);
        } catch(Exception jpe) {
            Throwables.propagate(jpe);
        }
        return response;
    }

    @Override
    public ValueType apply(KeyType key) {
        return getUnchecked(key);
    }

    @Override
    public ValueType get(KeyType key) throws ExecutionException {
        try {
            return getCallback(key).getOrThrowUnchecked();
        } catch (Exception ex) {
            throw new ExecutionException(ex);
        }
    }

    protected ValueType load(KeyType key) {
        return loader.apply(key);
    }

    public CallbackResult<ValueType> getCallback(final KeyType key) {
        // Attempt to load cached data for company from redis. Use new RDBI handle.
        CachedData<ValueType> cachedData = getCachedDataNewJedis(key);
        if (cachedData != null) {
            log.debug("{}: Found cached data for {}", cacheName, key);
            // If the cached data we found is about to expire, attempt to refresh it asynchronously
            if (cachedData.getSecondsToLive() < cacheRefreshThresholdSecs) {
                // attempted will be false if another thread (or jvm) has acquired the lock key in redis for this company
                // it will be true if this thread was able to acquire the lock key.
                refresh(key);
            }
            // Regardless of expiration, we got some data so return it.
            markHit();
            return new CallbackResult<>(cachedData.getData());
        }

        // Bad news friend: we don't have anything in the redis cache. Load data and wait until we get it.
        markMiss();
        return loadDataSynchronously(key);
    }

    @Override
    public ValueType getUnchecked(KeyType key) {
        // We don't throw checked exceptions in this class.
        return getCallback(key).getOrThrowUnchecked();
    }

    /**
     * Attempts to get cached data normally, but if another thread
     * has a refresh lock, wait up to maxWaitMillis for the refresh.
     * This thread will poll every 250ms to see if cached data is
     * present.
     *
     * to complete.
     * @param key
     * @param maxWaitMillis
     * @return
     */
    public CallbackResult<ValueType> getPatiently(KeyType key, long maxWaitMillis) {
        CallbackResult<ValueType> result = getCallback(key);
        if (result.getError() instanceof LockUnavailableException) {
            try {
                long startTime = System.currentTimeMillis();
                boolean stopTrying = false;
                while (System.currentTimeMillis() - startTime < maxWaitMillis) {
                    Thread.sleep(250L);
                    if (!isLocked(key)) {
                        // Allow 1 last attempt.
                        stopTrying = true;
                    }
                    CachedData<ValueType> cached = getCachedDataNewJedis(key);
                    if (cached != null) {
                        return new CallbackResult<>(cached.getData());
                    }
                    if (stopTrying) {
                        break;
                    }
                }
                // We have exceeded the max time to wait. Fall through to service unavailable.
            } catch(InterruptedException ie) {
                log.info("{}: Thread interrupted", cacheName, ie);
                Thread.currentThread().interrupt();
                // Fall through to service unavailable.
            }
            return new CallbackResult<>(new ServiceUnavailableException());
        } else {
            return result;
        }
    }

    @Override
    public void refresh(final KeyType key) {
        asyncService.submit(new AsyncCacheRefresher<>(this, rdbi, key));
    }

    @Override
    public ConcurrentMap<KeyType, ValueType> asMap() {
        // Guaranteed to return empty map. Nothing is in memory.
        return Maps.newConcurrentMap();
    }

    @Nullable
    @Override
    public ValueType getIfPresent(Object objKey) {
        if (objKey == null) {
            log.warn("{}: Null key provided", cacheName);
            return null;
        }
        KeyType key = (KeyType)objKey;
        CachedData<ValueType> cachedData = getCachedDataNewJedis(key);
        if (cachedData != null) {
            log.debug("{}: Found cached data: {}", cacheName, key);
            markHit();
        }
        markMiss();
        return null;
    }

    @Override
    public ValueType get(KeyType key, Callable<? extends ValueType> valueLoader) throws ExecutionException {
        CachedData<ValueType> cachedData = getCachedDataNewJedis(key);
        if (cachedData != null) {
            log.debug("{}: Found cached data: {}", cacheName, key);
            markHit();
            return cachedData.getData();
        }

        // this method doesn't acquire the write lock... sort of seems like a bug, but the API is sketchy here...
        // ideally we'd acquire the lock OR hang around until the value was loaded... something to consider...
        markMiss();
        long start = System.currentTimeMillis();
        try {
            ValueType result = valueLoader.call();
            put(key, result);
            markLoadSuccess(System.currentTimeMillis() - start);
            return result;
        } catch(Exception e) {
            log.error(cacheName + ": Failed to load for ", e);
            markLoadException(System.currentTimeMillis() - start);
            throw new ExecutionException(e);
        }
    }

    @Override
    public ImmutableMap<KeyType, ValueType> getAll(Iterable<? extends KeyType> keys) throws ExecutionException {
        // Guaranteed to return empty list. Nothing is in memory.
        return ImmutableMap.of();
    }

    @Override
    public ImmutableMap<KeyType, ValueType> getAllPresent(Iterable<?> keys) {
        return ImmutableMap.of();
    }

    @Override
    public void put(final KeyType key, final ValueType value) {
        rdbi.withHandle(new Callback<Object>() {
            @Override
            public Object run(Handle handle) {
                cacheData(key, handle.jedis(), value, valueTtl);
                return null;
            }
        });
    }

    @Override
    public void putAll(Map<? extends KeyType, ? extends ValueType> m) {
        for (Map.Entry<? extends KeyType, ? extends ValueType> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void invalidate(Object objKey) {
        if (objKey == null) {
            log.warn("{}: Invalidation requested for null key", cacheName);
            return;
        }
        KeyType key = (KeyType) objKey;
        final String redisKey = generateRedisKey(key);
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().del(redisKey);
                return null;
            }
        });
        cacheEvictionCount.incrementAndGet();
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        for(final Object key : keys) {
            invalidate(key);
        }
    }

    @Override
    public void invalidateAll() {
        log.error("{}: Must provide explicit set of keys", cacheName);
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public CacheStats stats() {
        return new CacheStats(
                cacheHitCount.get(),
                cacheMissCount.get(),
                cacheLoadSuccessCount.get(),
                cacheLoadExceptionCount.get(),
                cacheTotalLoadTime.get(),
                cacheEvictionCount.get());
    }

    @Override
    public void cleanUp() {
        log.info("{}: Cleanup not implemented.", cacheName);
    }
}
