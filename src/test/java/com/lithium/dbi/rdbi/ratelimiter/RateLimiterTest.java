package com.lithium.dbi.rdbi.ratelimiter;

import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;

public class RateLimiterTest {

    private static final Logger logger = LoggerFactory.getLogger(RateLimiterTest.class);

    @Test
    public void testSimpleRateLimitingWithSlowRate() {
        RateLimiter redisRateLimiter = buildRateLimiter(0.75); // 3 requests every 4s

        double rate = 0.0;
        long startTime = System.nanoTime();
        for (int i = 0; i < 4; ++i) {
            redisRateLimiter.acquire(true);
            rate++;
            logger.info("Acquired rate limit permit");
        }
        double meanRate = getMeanRate(rate, startTime);
        logger.info("Overall rate limit calls/sec {}", meanRate);

        // The rate limiter is a little "fuzzy", so our assertion of "good enough" is also a little fuzzy
        assertTrue(meanRate < 1.0 && meanRate > 0.5);
    }

    @Test
    public void testSimpleRateLimitingWithFastRate() {
        RateLimiter redisRateLimiter = buildRateLimiter(10); // 10 requests every sec

        double rate = 0.0;
        long startTime = System.nanoTime();
        for (int i = 0; i < 40; ++i) {
            redisRateLimiter.acquire(true);
            rate++;
            logger.info("Acquired rate limit permit");
        }
        double meanRate = getMeanRate(rate, startTime);
        logger.info("Overall rate limit calls/sec {}", meanRate);

        // The rate limiter is a little "fuzzy", so our assertion of "good enough" is also a little fuzzy
        assertTrue(meanRate < 13.5 && meanRate > 7.0);
    }

    private RateLimiter buildRateLimiter(double permitsPerSecond) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(30);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, "localhost", 6379, Protocol.DEFAULT_TIMEOUT);

        RDBI rdbi = new RDBI(jedisPool);

        // Verify our loading of the LUA script upon initial start.
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().scriptFlush();
                return null;
            }
        });

        return new RateLimiter("d:test:rdbi", rdbi, UUID.randomUUID().toString(), permitsPerSecond);
    }

    private double getMeanRate(double rate, long startTime) {
        if(rate == 0L) {
            return 0.0D;
        } else {
            double elapsed = (double)(System.nanoTime() - startTime);
            return rate / elapsed * (double) TimeUnit.SECONDS.toNanos(1L);
        }
    }
}
