package com.lithium.dbi.rdbi.recipes.locking;

import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Query;

public interface RedisSemaphoreDAO {
    @Query(
            "local acquiredSemaphore = redis.call('SETNX', $semaphoreKey$, $ownerId$)\n" +
            "if acquiredSemaphore == 1 then\n" +
            "   redis.call('EXPIRE', $semaphoreKey$, $timeout$)\n" +
            "end\n" +
            "return acquiredSemaphore\n"
    )
    int acquireSemaphore(
            @BindKey("semaphoreKey") String semaphoreKey,
            @BindArg("ownerId") String ownerId,
            @BindArg("timeout") Integer timeout);

    @Query(
            "local keyOwner = redis.call('GET', $semaphoreKey$)\n" +
            "if keyOwner == $ownerId$ then\n" +
            "   redis.call('DEL', $semaphoreKey$)\n" +
            "end\n" +
            "return keyOwner\n"
    )
    String releaseSemaphore(
            @BindKey("semaphoreKey") String semaphoreKey,
            @BindArg("ownerId") String ownerId);

    @Query(
            "local keyOwner = redis.call('GET', $semaphoreKey$)\n" +
            "if keyOwner == $ownerId$ then\n" +
            "   redis.call('EXPIRE', $semaphoreKey$, $timeout$)\n" +
            "end\n" +
            "return keyOwner\n"
    )
    String reacquireSemaphore(
            @BindKey("semaphoreKey") String semaphoreKey,
            @BindArg("ownerId") String ownerId,
            @BindArg("timeout") Integer timeout);
}

