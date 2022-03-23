package com.lithium.dbi.rdbi;

import org.apache.commons.pool2.impl.DefaultEvictionPolicy;
import org.apache.commons.pool2.impl.EvictionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class HandleTest {

    Logger logger = LoggerFactory.getLogger(HandleTest.class);

    @Test
    public void evictResourceTestRdbiMaintainsConnection() {
        final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        jedisPoolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(200));
        jedisPoolConfig.setMinEvictableIdleTime(Duration.ofMillis(200));
        AtomicLong evictCounter = new AtomicLong(0);
        jedisPoolConfig.setEvictionPolicy(chattyDefaultEvictionPolicy(evictCounter));

        JedisPool pool = new JedisPool(jedisPoolConfig,
                                       "localhost", 6379, 1000);
        RDBI rdbi = new RDBI(pool);

        AtomicInteger connRef = new AtomicInteger(-1);

        rdbi.withHandle(h -> {
            h.jedis().set("key", "value");
            connRef.set(System.identityHashCode(h.jedis().getConnection()));
            return h.jedis().get("key");
        });

        // normal usage maintains the underlying connection
        rdbi.withHandle(h -> {
            h.jedis().set("key", "value");
            assertEquals(connRef.get(), System.identityHashCode(h.jedis().getConnection()));
            return h.jedis().get("key");
        });

        // wait for eviction
        await().atMost(10, TimeUnit.SECONDS)
               .untilAsserted(() -> assertEquals(evictCounter.get(), 1));

        // once the connection is evicted, we get a new one.
        rdbi.withHandle(h -> {
            h.jedis().set("key", "value");
            int thisConnId = System.identityHashCode(h.jedis().getConnection());
            assertNotEquals(connRef.get(), thisConnId);
            connRef.set(thisConnId);
            return h.jedis().get("key");
        });

        // and continue to use it
        rdbi.withHandle(h -> {
            h.jedis().set("key", "value");
            assertEquals(connRef.get(), System.identityHashCode(h.jedis().getConnection()));
            return h.jedis().get("key");
        });
    }


    private <T> EvictionPolicy<T> chattyDefaultEvictionPolicy(AtomicLong evictCounter) {
        return (config, underTest, idleCount) -> {
            logger.info("testing eviction on {} @ {}", underTest, System.identityHashCode(underTest));
            boolean evict = new DefaultEvictionPolicy<T>().evict(config, underTest, idleCount);
            logger.info("DefaultEvictionPolicy says evict = {}", evict);
            if (evict) {
                evictCounter.incrementAndGet();
            }
            return evict;
        };
    }
}
