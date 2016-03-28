package com.lithium.dbi.rdbi.recipes.presence;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;

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

        Handle handle = rdbi.open();

        try {
            handle.jedis().zadd(getQueue(tube),  now.getMillis() + timeToExpireInMS, id);
        } finally {
            handle.close();
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
                return handle.jedis().zrangeByScore(getQueue(tube), Long.toString(now.getMillis()), "+inf", 0, limit.get());
            } else {
                return handle.jedis().zrangeByScore(getQueue(tube), Long.toString(now.getMillis()), "+inf");
            }
        }
    }

    public Set<String> getExpired(String tube, Optional<Integer> limit) {
        final Instant now = Instant.now();

        try (final Handle handle = rdbi.open()) {
            if (limit != null && limit.isPresent()) {
                return handle.jedis().zrangeByScore(getQueue(tube), "-inf", Long.toString(now.getMillis()), 0, limit.get());
            } else {
                return handle.jedis().zrangeByScore(getQueue(tube), "-inf", Long.toString(now.getMillis()));
            }
        }
    }

    public boolean expired(String tube, String id) {

        Instant now = Instant.now();

        Handle handle = rdbi.open();
        try {
            Double score = handle.jedis().zscore(getQueue(tube), id);
            if (score == null ) {
                return true;
            } else if (score < Double.valueOf(now.getMillis())) {
                return true;
            } else {
                return false;
            }
        } finally {
            handle.close();
        }
    }

    public boolean remove(String tube, String id) {
        Handle handle = rdbi.open();
        try {
            return 0L < handle.jedis().zrem(getQueue(tube), id);
        } finally {
            handle.close();
        }
    }

    public void cull(String tube) {
        Instant now = Instant.now();

        Handle handle = rdbi.open();
        try {
            handle.jedis().zremrangeByScore(getQueue(tube), 0, now.getMillis());
        } finally {
            handle.close();
        }
    }

    @VisibleForTesting
    void nukeForTest(final String tube) {
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().del(getQueue(tube));
                return null;
            }
        });
    }

    private String getQueue(String tube) {
        return prefix + tube;
    }
}
