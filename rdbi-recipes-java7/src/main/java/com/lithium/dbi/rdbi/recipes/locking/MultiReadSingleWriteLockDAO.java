package com.lithium.dbi.rdbi.recipes.locking;

import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Query;

public interface MultiReadSingleWriteLockDAO {
    @Query(
            "local writeLock = redis.call('SETNX', $writeLockKey$, $ownerId$)\n" +
            "if writeLock == 1 then\n" +
            "   redis.call('PEXPIRE', $writeLockKey$, $timeoutInMillis$)\n" +
            "end\n" +
            "return writeLock\n"
    )
    int acquireWriteLock(@BindKey("writeLockKey") String writeLockKey,
                         @BindArg("ownerId") String ownerId,
                         @BindArg("timeoutInMillis") Integer timeoutInMillis);

    @Query(
            "local writeLockOwner = redis.call('GET', $writeLockKey$)\n" +
            "if writeLockOwner == $ownerId$ then\n" +
            "    return redis.call('DEL', $writeLockKey$)\n" +
            "end\n" +
            "return 0"
    )
    int releaseWriteLock(@BindKey("writeLockKey") String writeLockKey,
                         @BindArg("ownerId") String ownerId);

    @Query(
            "local writeLockOwner = redis.call('GET', $writeLockKey$)\n" +
            "if writeLockOwner then\n" +
            "    return 0\n" +
            "end\n" +
            "return redis.call('ZADD', $readLockKey$, $expirationEpoch$, $ownerId$)"
    )
    int acquireReadLock(@BindKey("writeLockKey") String writeLockKey,
                        @BindKey("readLockKey") String readLockKey,
                        @BindArg("ownerId") String ownerId,
                        @BindArg("expirationEpoch") Long expirationEpoch);

    @Query(
            "local releasedLock = redis.call('ZREM', $readLockKey$, $ownerId$)\n" +
            "local readerCount = redis.call('ZCOUNT', $readLockKey$, $currentMillis$, '+inf')\n" +
            "if readerCount < 1 then\n" +
            "    redis.call('DEL', $readLockKey$)\n" +
            "end\n" +
            "return releasedLock"
    )
    int releaseReadLock(@BindKey("readLockKey") String readLockKey,
                        @BindArg("currentMillis") Long currentMillis,
                        @BindArg("ownerId") String ownerId);

}
