package com.lithium.dbi.rdbi.recipes.locking;

import com.lithium.dbi.rdbi.Query;

import java.util.List;

public interface SemaphoreDAO {
    @Query(
            "local acquiredLock = redis.call('SETNX', KEYS[1], ARGV[1])\n" +
            "if acquiredLock == 1 then\n" +
            "   redis.call('EXPIRE', KEYS[1], ARGV[2])\n" +
            "end\n" +
            "return acquiredLock\n"
    )
    int acquireLock(List<String> keys, List<String> args);

    @Query(
            "local keyOwner = redis.call('GET', KEYS[1])\n" +
            "if keyOwner == ARGV[1] then\n" +
            "   redis.call('DEL', KEYS[1])\n" +
            "end\n" +
            "return keyOwner\n"
    )
    String releaseLock(List<String> keys, List<String> args);
}