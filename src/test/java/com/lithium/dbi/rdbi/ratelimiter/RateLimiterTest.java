package com.lithium.dbi.rdbi.ratelimiter;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
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

import static org.testng.Assert.assertTrue;

public class RateLimiterTest {

    private static final Logger logger = LoggerFactory.getLogger(RateLimiterTest.class);

    @Test
    public void testSimpleRateLimitingWithSlowRate() {
        RateLimiterFactory redisRateLimiterFactory = buildRateLimiterFactory();
        RateLimiter redisRateLimiter = redisRateLimiterFactory.getInstance(UUID.randomUUID().toString(), 0.75); // 3 requests every 4s

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter callsPerSec = metricRegistry.meter("callspersec");
        for (int i = 0; i < 4; ++i) {
            redisRateLimiter.acquire(true);
            callsPerSec.mark();
            logger.info("Acquired rate limit permit");
        }
        logger.info("Overall rate limit calls/sec {}", callsPerSec.getMeanRate());

        // The rate limiter is a little "fuzzy", so our assertion of "good enough" is also a little fuzzy
        assertTrue(callsPerSec.getMeanRate() < 1.0 && callsPerSec.getMeanRate() > 0.5);
    }

    @Test
    public void testSimpleRateLimitingWithFastRate() {
        RateLimiterFactory redisRateLimiterFactory = buildRateLimiterFactory();
        RateLimiter redisRateLimiter = redisRateLimiterFactory.getInstance(UUID.randomUUID().toString(), 10); // 10 requests every sec

        MetricRegistry metricRegistry = new MetricRegistry();
        Meter callsPerSec = metricRegistry.meter("callspersec");
        for (int i = 0; i < 40; ++i) {
            redisRateLimiter.acquire(true);
            callsPerSec.mark();
            logger.info("Acquired rate limit permit");
        }
        logger.info("Overall rate limit calls/sec {}", callsPerSec.getMeanRate());

        // The rate limiter is a little "fuzzy", so our assertion of "good enough" is also a little fuzzy
        assertTrue(callsPerSec.getMeanRate() < 13.5 && callsPerSec.getMeanRate() > 7.0);
    }

    private RateLimiterFactory buildRateLimiterFactory() {
        RateLimiterConfiguration rateLimiterConfiguration = new RateLimiterConfiguration();
        rateLimiterConfiguration.setKeyPrefix("d:test:rdbi");
        rateLimiterConfiguration.setRedisDb(0);
        rateLimiterConfiguration.setRedisHostAndPort("localhost:6379");

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

        return new RateLimiterFactory(rateLimiterConfiguration, rdbi);
    }

}
