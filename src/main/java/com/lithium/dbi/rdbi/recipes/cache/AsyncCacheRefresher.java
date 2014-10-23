package com.lithium.dbi.rdbi.recipes.cache;

import com.lithium.dbi.rdbi.RDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AsyncCacheRefresher<KeyType, ValueType> implements Future<CallbackResult<ValueType>>, Callable<CallbackResult<ValueType>> {
    private static final Logger log = LoggerFactory.getLogger(RDBI.class);
    private final RedisCache<KeyType, ValueType> cache;
    private final RDBI rdbi;
    private final KeyType key;
    private volatile boolean cancelled;
    private BlockingReference<CallbackResult<ValueType>> blockingReference;

    protected AsyncCacheRefresher(final RedisCache<KeyType, ValueType> cache,
                                  final RDBI rdbi,
                                  final KeyType key) {
        this.cache = cache;
        this.rdbi = rdbi;
        this.key = key;
        this.blockingReference = new BlockingReference<>();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if(cancelled) {
            return true;
        }

        // to REALLY do this in a robust way we'd have to make sure *we* were the ones that grabbed the lock from
        // Redis. Pretty sure the semaphone provides a great foundation for doing this, need to move to it eventually.
        cache.releaseLock(key);
        cancelled = true;
        return true;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isDone() {
        if(cancelled || blockingReference.peek() != null) {
            return true;
        }
        return false;
    }

    @Override
    public CallbackResult<ValueType> get() throws InterruptedException, ExecutionException {
        return blockingReference.get();
    }

    @Override
    public CallbackResult<ValueType> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return blockingReference.get(TimeUnit.MILLISECONDS.convert(timeout, unit));
    }

    @Override
    public CallbackResult<ValueType> call() throws Exception {
        try {
            log.debug("{}: Attempting to refresh data asynchonously for", cache.getCacheName());
            final ValueType value = cache.load(key);
            final CallbackResult<ValueType> result = new CallbackResult<>(value);
            cache.put(key, value); // this shouldn't throw, the withHandle eats it...
            blockingReference.set(result); // unconditional write, most recent writer will win
            return result;
        } catch (Exception ex) {
            return new CallbackResult<>(ex);
        } finally {
            cache.releaseLock(key);
        }
    }
}
