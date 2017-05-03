package com.lithium.dbi.rdbi.ratelimiter;

import com.google.common.util.concurrent.Uninterruptibles;
import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class TokenBucketRateLimiterTest {


    private RDBI rdbi = getRdbi();

    @Test
    public void testBasics() {
        // clock that advances 100ms / tick
        long start = 10_000L;
        TestClock clock = new TestClock(start, 100);

        // limiter with refill rate of 2/second
        // burstable up to 20 requests

        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 20, 2, TimeUnit.SECONDS);

        // we can get 20 permits at the same time
        for (int i = 0; i < 20; i++) {
            assertFalse(tokenBucketRateLimiter.getOptionalWaitTimeForPermit().isPresent());
        }
        // but not 1 more
        OptionalLong oneMore = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 500ms to get 1 more
        assertTrue(oneMore.getAsLong() == 500L);

        // waiting 400ms won't do it
        for (int i = 1; i < 5; i++) {
            clock.tick();
            OptionalLong nextCheck = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
            assertTrue(nextCheck.isPresent());

            // each iteration has us sleeping 100ms less
            assertTrue(nextCheck.getAsLong() == 500L - (i * 100));
        }

        // and then finally we can get one
        clock.tick();
        assertFalse(tokenBucketRateLimiter.getOptionalWaitTimeForPermit().isPresent());

        // but not 1 more
        oneMore = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 500ms to get 1 more
        assertTrue(oneMore.getAsLong() == 500L);

        // but then if we wait for the refill, 2/second at 100ms per tick, we need 10 seconds to fully refill
        for (int i = 0; i < 10 * 10; i++) {
            clock.tick();
        }

        // then we can get 20 again
        // we can get 20 permits at the same time
        for (int i = 0; i < 20; i++) {
            assertFalse(tokenBucketRateLimiter.getOptionalWaitTimeForPermit().isPresent());
        }
        // but not 1 more
        oneMore = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
        assertTrue(oneMore.isPresent());
    }


    @Test
    public void testLongRunAverageWithSmallAdvance() {

        // if we have persistent requests over a longer time
        // period, the total number should not be more than
        // our refill rate plus bucket size
        // clock that advances 100ms / tick
        long start = 10_000L;
        TestClock clock = new TestClock(start, 1);

        // limiter with refill rate of 2/second
        // burstable up to 2 requests
        // if we run for a full 30 seconds, requesting every millisecond
        // our total granted permits should be 64
        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 4, 2, TimeUnit.SECONDS);

        int granted = 0;
        for (int i = 0; i <= 30_000; i++) {
            OptionalLong nextCheck = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();

            if (!nextCheck.isPresent()) {
                granted+=1;
            }
            clock.tick();
        }
        assertEquals(granted, 64);
    }

    @Test
    public void testEmulateNonBurstable() {
        // clock that advances 100ms / tick
        long start = 10_000L;
        TestClock clock = new TestClock(start, 100);

        // limiter with refill rate of 2/second
        // burstable up to 2 requests

        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 2, 2, TimeUnit.SECONDS);

        // we can get 20 permits at the same time
        for (int i = 0; i < 2; i++) {
            assertFalse(tokenBucketRateLimiter.getOptionalWaitTimeForPermit().isPresent());
        }
        // but not 1 more
        OptionalLong oneMore = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 500ms to get 1 more
        assertTrue(oneMore.getAsLong() == 500L);

        // waiting 400ms won't do it
        for (int i = 1; i < 5; i++) {
            clock.tick();
            OptionalLong nextCheck = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
            assertTrue(nextCheck.isPresent());

            // each iteration has us sleeping 100ms less
            assertTrue(nextCheck.getAsLong() == 500L - (i * 100));
        }

        // and then finally we can get one
        clock.tick();
        assertFalse(tokenBucketRateLimiter.getOptionalWaitTimeForPermit().isPresent());

        // but not 1 more
        oneMore = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 500ms to get 1 more
        assertTrue(oneMore.getAsLong() == 500L);
    }

    @Test
    public void testMultiPermit() {
        // clock that advances 100ms / tick
        long start = 10_000L;
        TestClock clock = new TestClock(start, 100);

        // limiter with refill rate of 2/second
        // burstable up to 20 requests

        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 20, 2, TimeUnit.SECONDS);

        // we can get 20 permits at the same time
        assertFalse(tokenBucketRateLimiter.getOptionalWaitTimeForPermits(20).isPresent());
        // but not 1 more
        OptionalLong oneMore = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 500ms to get 1 more
        assertTrue(oneMore.getAsLong() == 500L);

        // waiting 400ms won't do it
        for (int i = 1; i < 5; i++) {
            clock.tick();
            OptionalLong nextCheck = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
            assertTrue(nextCheck.isPresent());

            // each iteration has us sleeping 100ms less
            assertTrue(nextCheck.getAsLong() == 500L - (i * 100));
        }

        // and then finally we can get one
        clock.tick();
        assertFalse(tokenBucketRateLimiter.getOptionalWaitTimeForPermit().isPresent());

        // but not 1 more
        oneMore = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 500ms to get 1 more
        assertTrue(oneMore.getAsLong() == 500L);
    }


    @Test
    public void testDifferentTimePeriods() {
        // clock that advances 1000ms / tick
        long start = 10_000L;
        TestClock clock = new TestClock(start, 1000);

        // limiter with refill rate of 2/minute
        // burstable up to 20 requests

        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 20, 2, TimeUnit.MINUTES);

        // we can get 20 permits at the same time
        assertFalse(tokenBucketRateLimiter.getOptionalWaitTimeForPermits(20).isPresent());

        // but not 1 more
        OptionalLong oneMore = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 30s to get 1 more
        assertTrue(oneMore.getAsLong() == 30_000L);

        // waiting 29s won't do it
        for (int i = 1; i < 30; i++) {
            clock.tick();
            OptionalLong nextCheck = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
            assertTrue(nextCheck.isPresent());

            // each iteration has us sleeping 100ms less
            assertTrue(nextCheck.getAsLong() == 30_000L - (i * 1000L));
        }

        // and then finally we can get one
        clock.tick();
        assertFalse(tokenBucketRateLimiter.getOptionalWaitTimeForPermit().isPresent());

        // but not 1 more
        oneMore = tokenBucketRateLimiter.getOptionalWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 30s to get 1 more
        assertTrue(oneMore.getAsLong() == 30_000L);
    }

    @Test
    public void testExpire() {
        // clock that advances 1000ms / tick
        long start = 10_000L;
        TestClock clock = new TestClock(start, 1000);

        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 1, 1, TimeUnit.SECONDS);

        assertFalse(tokenBucketRateLimiter.getOptionalWaitTimeForPermit().isPresent());
        // creates an entry that should expire in 40ms
        try (Handle handle = rdbi.open()) {
            final Jedis jedis = handle.jedis();
            assertEquals(jedis.ttl(tokenBucketRateLimiter.getKey()).longValue(), 2L);
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
            assertFalse(jedis.exists(tokenBucketRateLimiter.getKey()));
        }
    }

    @Test
    public void testRealTime() {
        // do it live
        // 10 max,
        // 10 per second refill rate
        // so ~ 40 requests in 3 seconds or ~ 13.333

        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(System::currentTimeMillis, 10, 10, TimeUnit.SECONDS);
        Limiter blocking = new BlockingRateLimiter(tokenBucketRateLimiter);
        double rate = 0.0;
        long startTime = System.nanoTime();
        for (int i = 0; i < 40; ++i) {
            assertTrue(blocking.acquire());
            rate++;
        }
        double meanRate = getMeanRate(rate, startTime);
        assertTrue(meanRate < 13.5 && meanRate > 10.0);

        // if we do 20 more, our bucket should be 'empty'
        // so will take about 2 more seconds, or ~12 per second

        for (int i = 0; i < 20; ++i) {
            assertTrue(blocking.acquire());
            rate++;
        }
        double meanRate2 = getMeanRate(rate, startTime);
        assertTrue(meanRate2 < 12.5 && meanRate2 < meanRate && meanRate2 > 10.0);


    }

    private double getMeanRate(double rate, long startTime) {
        if(rate == 0L) {
            return 0.0D;
        } else {
            double elapsed = (double)(System.nanoTime() - startTime);
            return rate / elapsed * (double) TimeUnit.SECONDS.toNanos(1L);
        }
    }


    private TokenBucketRateLimiter buildRateLimiter(LongSupplier clock, int maxTokens, int requestRate, TimeUnit refillPeriod) {

        return new TokenBucketRateLimiter(
                rdbi,
                "d:test:rdbi",
                UUID.randomUUID().toString(),
                maxTokens,
                requestRate,
                refillPeriod,
                clock);
    }

    private RDBI getRdbi() {
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
        return rdbi;
    }

    private static class TestClock implements LongSupplier {
        private final long interval;
        private long now;

        TestClock(long start, long interval) {
            this.now = start;
            this.interval = interval;
        }

        void tick() {
            now += interval;
        }

        @Override
        public long getAsLong() {
            return now;
        }
    }
}