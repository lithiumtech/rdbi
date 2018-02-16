package com.lithium.dbi.rdbi.recipes.presence;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;

import java.time.Instant;
import java.util.Set;

public class PresenceRepository {

    private final RDBI rdbi;
    private final String prefix;

    public PresenceRepository(RDBI rdbi, String redisPrefixKey) {
        this.rdbi = rdbi;
        this.prefix = redisPrefixKey;
    }

    public void addHeartbeat(String tube, String id, long timeToExpireInMS) {
        Instant now = Instant.now();
        try (Handle handle = rdbi.open()) {
            handle.jedis().zadd(getQueue(tube), now.toEpochMilli() + timeToExpireInMS, id);
        }
    }

    /**
     * Get all entries that have not expired
     * @param tube name of the tube
     * @param limit provide a max number of entries to return, will return all if not provided
     * @return all entries that have not expired
     */
    public Set<String> getPresent(String tube, Optional<Integer> limit) {
        final Instant now = Instant.now();

        try (final Handle handle = rdbi.open()) {
            if (limit != null && limit.isPresent()) {
                return handle.jedis().zrangeByScore(getQueue(tube), Long.toString(now.toEpochMilli()), "+inf", 0, limit.get());
            } else {
                return handle.jedis().zrangeByScore(getQueue(tube), Long.toString(now.toEpochMilli()), "+inf");
            }
        }
    }

    /**
     * Get all entries that have expired
     * @param tube name of the tube
     * @param limit provide a max number of entries to return, will return all if not provided
     * @return all entries that have expired
     */
    public Set<String> getExpired(String tube, Optional<Integer> limit) {
        final Instant now = Instant.now();

        try (final Handle handle = rdbi.open()) {
            if (limit != null && limit.isPresent()) {
                return handle.jedis().zrangeByScore(getQueue(tube), "-inf", Long.toString(now.toEpochMilli()), 0, limit.get());
            } else {
                return handle.jedis().zrangeByScore(getQueue(tube), "-inf", Long.toString(now.toEpochMilli()));
            }
        }
    }

    public boolean expired(String tube, String id) {

        Instant now = Instant.now();

        try (Handle handle = rdbi.open()) {
            Double score = handle.jedis().zscore(getQueue(tube), id);
            if (score == null) {
                return true;
            } else if (score < (double) now.toEpochMilli()) {
                return true;
            } else {
                return false;
            }
        }
    }

    public boolean remove(String tube, String id) {
        try (Handle handle = rdbi.open()) {
            return 0L < handle.jedis().zrem(getQueue(tube), id);
        }
    }

    public void cull(String tube) {
        Instant now = Instant.now();

        try (Handle handle = rdbi.open()) {
            handle.jedis().zremrangeByScore(getQueue(tube), 0, now.toEpochMilli());
        }
    }

    @VisibleForTesting
    void nukeForTest(final String tube) {
        rdbi.withHandle((Callback<Void>) handle -> {
            handle.jedis().del(getQueue(tube));
            return null;
        });
    }

    private String getQueue(String tube) {
        return prefix + tube;
    }
}
