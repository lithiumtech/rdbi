package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.recipes.locking.RedisSemaphoreDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class RedisCache<KeyType, ValueType> extends AbstractRedisCache<KeyType, ValueType> implements LockingInstrumentedCache<KeyType, ValueType> {

    private static final Logger log = LoggerFactory.getLogger(RedisCache.class);

    private final KeyGenerator<KeyType> redisKeyGenerator;
    private final SerializationHelper<ValueType> serializationHelper;
    private final String keyPrefix;
    private final int valueTtl;
    private final long cacheRefreshThresholdSecs;
    private final int lockTimeoutSecs;
    private final int lockReleaseRetries;
    private final long lockReleaseRetryWaitMillis;

    /**
     * @param keyGenerator - something that will turn your key object into a string redis can use as a key.
     *                     will be prefixed by the keyPrefix string.
     * @param serializationHelper - a codec to get your value object to and from a string
     * @param rdbi - an RDBI instance
     * @param loader - function to go get your object
     * @param cacheName - name of cache, used in log statements
     * @param keyPrefix - prefix of all keys used by this cache in redis
     * @param valueTtlSecs - redis entries holding your values will expire after this many seconds
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
    public RedisCache(KeyGenerator<KeyType> keyGenerator,
                      SerializationHelper<ValueType> serializationHelper,
                      RDBI rdbi,
                      Function<KeyType, ValueType> loader,
                      String cacheName,
                      String keyPrefix,
                      int valueTtlSecs,
                      long cacheRefreshThresholdSecs,
                      int lockTimeoutSecs,
                      Optional<ExecutorService> asyncService,
                      Runnable hitAction,
                      Runnable missAction,
                      Runnable loadSuccessAction,
                      Runnable loadExceptionAction) {
        super(cacheName, loader, rdbi, asyncService, hitAction, missAction, loadSuccessAction, loadExceptionAction);
        this.redisKeyGenerator = keyGenerator;
        this.serializationHelper = serializationHelper;
        this.keyPrefix = keyPrefix;
        this.valueTtl = valueTtlSecs;
        this.cacheRefreshThresholdSecs = cacheRefreshThresholdSecs;
        this.lockTimeoutSecs = lockTimeoutSecs;
        lockReleaseRetries = 3;
        lockReleaseRetryWaitMillis = TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS);
    }

    String redisLockKey(String redisKey) {
        return redisKey + ":lock";
    }

    @Override
    public boolean acquireLock(final KeyType key) {
        return rdbi.withHandle(new Callback<Boolean>() {
            @Override
            public Boolean run(Handle handle) {
                return 1 == handle.attach(RedisSemaphoreDAO.class)
                                  .acquireSemaphore(redisLockKey(generateRedisKey(key)),
                                                    String.valueOf(System.currentTimeMillis()),
                                                    lockTimeoutSecs);
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

    private CallbackResult<ValueType> loadDataSynchronously(final KeyType key) {
        return new AsyncCacheRefresher<>(this, key).call();
    }

    private void releaseLock(final KeyType key, final Jedis jedis) {
        for(int i = 0; i < lockReleaseRetries; i++) {
            try {
                jedis.del(redisLockKey(generateRedisKey(key)));
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
    public void releaseLock(final KeyType key) {
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

    /**
     * Attempts to get cached data normally, but if another thread
     * has a refresh lock, wait up to maxWaitMillis for the refresh.
     * This thread will poll every 250ms to see if cached data is
     * present.
     *
     * to complete.
     * @param key Key to load a value for.
     * @param maxWaitMillis max time in milliseconds to wait if another thread is loading the value.
     * @return a result for the value associated with the key.
     */
    public CallbackResult<ValueType> getPatiently(KeyType key, long maxWaitMillis) {
        CallbackResult<ValueType> result = getCallback(key);
        if (result.getError() instanceof LockUnavailableException) {
            result = getFromCacheOrWait(key, maxWaitMillis);
        }
        return result;
    }


    /**
     * Attempts to get cached data normally, but if another thread
     * has a refresh lock, wait up to maxWaitMillis for the refresh.
     * This thread will poll every 250ms to see if cached data is
     * present.
     *
     * If we have not acquired the data after maxWaitMillis, then
     * attempt to load data without write lock.
     *
     * @param key key to load a value for.
     * @param maxWaitMillis max time in milliseconds to wait if another thread is loading the value.
     * @return a result for the value associated with the key.
     */
    public CallbackResult<ValueType> getPatientlyThenForcibly(KeyType key, long maxWaitMillis) {
        CallbackResult<ValueType> result = getCallback(key);
        if (result.getError() instanceof LockUnavailableException) {
            result = getFromCacheOrWait(key, maxWaitMillis);
            if (result.getError() instanceof ServiceUnavailableException) {
                result = new AsyncLockFreeRefresher<>(this, key).call();
            }
        }
        return result;
    }

    private CallbackResult<ValueType> getFromCacheOrWait(KeyType key, long maxWaitMillis) {
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
        log.warn("{}: Max wait time expired: {}", getCacheName(), maxWaitMillis);
        return new CallbackResult<>(new ServiceUnavailableException());
    }

    @Override
    public void refresh(final KeyType key) {
        AsyncCacheRefresher<KeyType, ValueType> asyncCacheRefresher = new AsyncCacheRefresher<>(this, key);
        if(asyncService.isPresent()) {
            asyncService.get().submit(asyncCacheRefresher);
        } else {
            asyncCacheRefresher.call();
        }
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
            // We found some data, so return it!
            return cachedData.getData();
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
    public void cleanUp() {
        log.info("{}: Cleanup not implemented.", cacheName);
    }
}
