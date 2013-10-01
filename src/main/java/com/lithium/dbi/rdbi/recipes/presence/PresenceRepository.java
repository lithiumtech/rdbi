package com.lithium.dbi.rdbi.recipes.presence;

import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;

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

    public void cull(String tube) {
        Instant now = Instant.now();

        Handle handle = rdbi.open();
        try {
            handle.jedis().zremrangeByScore(getQueue(tube), 0, now.getMillis());
        } finally {
            handle.close();
        }
    }

    private String getQueue(String tube) {
        return prefix + tube;
    }
}
