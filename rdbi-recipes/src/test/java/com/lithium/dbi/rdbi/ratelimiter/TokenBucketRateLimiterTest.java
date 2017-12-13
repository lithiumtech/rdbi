package com.lithium.dbi.rdbi.ratelimiter;

import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.awaitility.Awaitility.await;
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

        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 20, 2, Duration.ofSeconds(1));

        // we can get 20 permits at the same time
        for (int i = 0; i < 20; i++) {
            assertFalse(tokenBucketRateLimiter.getWaitTimeForPermit().isPresent());
        }
        // but not 1 more
        OptionalLong oneMore = tokenBucketRateLimiter.getWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 500ms to get 1 more
        assertTrue(oneMore.getAsLong() == 500L);

        // waiting 400ms won't do it
        for (int i = 1; i < 5; i++) {
            clock.tick();
            OptionalLong nextCheck = tokenBucketRateLimiter.getWaitTimeForPermit();
            assertTrue(nextCheck.isPresent());

            // each iteration has us sleeping 100ms less
            assertTrue(nextCheck.getAsLong() == 500L - (i * 100));
        }

        // and then finally we can get one
        clock.tick();
        assertFalse(tokenBucketRateLimiter.getWaitTimeForPermit().isPresent());

        // but not 1 more
        oneMore = tokenBucketRateLimiter.getWaitTimeForPermit();
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
            assertFalse(tokenBucketRateLimiter.getWaitTimeForPermit().isPresent());
        }
        // but not 1 more
        oneMore = tokenBucketRateLimiter.getWaitTimeForPermit();
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
        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 4, 2, Duration.ofSeconds(1));

        int granted = 0;
        for (int i = 0; i <= 30_000; i++) {
            OptionalLong nextCheck = tokenBucketRateLimiter.getWaitTimeForPermit();

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

        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 2, 2, Duration.ofSeconds(1));

        // we can get 20 permits at the same time
        for (int i = 0; i < 2; i++) {
            assertFalse(tokenBucketRateLimiter.getWaitTimeForPermit().isPresent());
        }
        // but not 1 more
        OptionalLong oneMore = tokenBucketRateLimiter.getWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 500ms to get 1 more
        assertTrue(oneMore.getAsLong() == 500L);

        // waiting 400ms won't do it
        for (int i = 1; i < 5; i++) {
            clock.tick();
            OptionalLong nextCheck = tokenBucketRateLimiter.getWaitTimeForPermit();
            assertTrue(nextCheck.isPresent());

            // each iteration has us sleeping 100ms less
            assertTrue(nextCheck.getAsLong() == 500L - (i * 100));
        }

        // and then finally we can get one
        clock.tick();
        assertFalse(tokenBucketRateLimiter.getWaitTimeForPermit().isPresent());

        // but not 1 more
        oneMore = tokenBucketRateLimiter.getWaitTimeForPermit();
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

        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 20, 2, Duration.ofSeconds(1));

        // we can get 20 permits at the same time
        assertFalse(tokenBucketRateLimiter.getWaitTimeForPermits(20).isPresent());
        // but not 1 more
        OptionalLong oneMore = tokenBucketRateLimiter.getWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 500ms to get 1 more
        assertTrue(oneMore.getAsLong() == 500L);

        // waiting 400ms won't do it
        for (int i = 1; i < 5; i++) {
            clock.tick();
            OptionalLong nextCheck = tokenBucketRateLimiter.getWaitTimeForPermit();
            assertTrue(nextCheck.isPresent());

            // each iteration has us sleeping 100ms less
            assertTrue(nextCheck.getAsLong() == 500L - (i * 100));
        }

        // and then finally we can get one
        clock.tick();
        assertFalse(tokenBucketRateLimiter.getWaitTimeForPermit().isPresent());

        // but not 1 more
        oneMore = tokenBucketRateLimiter.getWaitTimeForPermit();
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

        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 20, 2, Duration.ofMinutes(1));

        // we can get 20 permits at the same time
        assertFalse(tokenBucketRateLimiter.getWaitTimeForPermits(20).isPresent());

        // but not 1 more
        OptionalLong oneMore = tokenBucketRateLimiter.getWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 30s to get 1 more
        assertEquals(oneMore.getAsLong(), 30_000L);

        // waiting 29s won't do it
        for (int i = 1; i < 30; i++) {
            clock.tick();
            OptionalLong nextCheck = tokenBucketRateLimiter.getWaitTimeForPermit();
            assertTrue(nextCheck.isPresent());

            // each iteration has us sleeping 100ms less
            assertTrue(nextCheck.getAsLong() == 30_000L - (i * 1000L));
        }

        // and then finally we can get one
        clock.tick();
        assertFalse(tokenBucketRateLimiter.getWaitTimeForPermit().isPresent());

        // but not 1 more
        oneMore = tokenBucketRateLimiter.getWaitTimeForPermit();
        assertTrue(oneMore.isPresent());

        // we should wait 30s to get 1 more
        assertTrue(oneMore.getAsLong() == 30_000L);
    }

    @Test
    public void testExpire() {
        // clock that advances 1000ms / tick
        long start = 10_000L;
        TestClock clock = new TestClock(start, 1000);

        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 1, 1, Duration.ofSeconds(1));

        assertFalse(tokenBucketRateLimiter.getWaitTimeForPermit().isPresent());
        // creates an entry that should expire in 2s
        try (Handle handle = rdbi.open()) {
            final Jedis jedis = handle.jedis();
            assertEquals(jedis.ttl(tokenBucketRateLimiter.getKey()).longValue(), 2L);
            await().atMost(5, TimeUnit.SECONDS).until(() -> !jedis.exists(tokenBucketRateLimiter.getKey()));
        }
    }

    @Test
    public void testAcquireTimesOut() {
        // clock that advances 100ms / tick
        long start = 10_000L;
        TestClock clock = new TestClock(start, 100);

        // limiter with refill rate of 2/second
        // burstable up to 20 requests
        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(clock, 20, 2, Duration.ofSeconds(1));

        // we can get 20 permits at the same time
        assertFalse(tokenBucketRateLimiter.getWaitTimeForPermits(20).isPresent());

        // but not 1 more
        long now = System.currentTimeMillis();
        assertFalse(tokenBucketRateLimiter.acquirePatiently(Duration.ofMillis(500)));
        assertTrue("at least 500ms passed before the method returned", System.currentTimeMillis() - now >= 500);
    }

    @Test
    public void testAcquireTimesOutButNotTooLong() {
        // clock that advances 100ms / tick
        long start = 10_000L;
        TestClock clock = new TestClock(start, 100);

        // limiter with refill rate of 2/second
        // burstable up to 20 requests
        TokenBucketRateLimiter tokenBucketRateLimiter = buildRateLimiter(System::currentTimeMillis, 20, 1, Duration.ofMinutes(1));

        // we can get 20 permits at the same time
        assertFalse(tokenBucketRateLimiter.getWaitTimeForPermits(20).isPresent());

        // but not 1 more.. and the wait time should be near 1 minute
        OptionalLong waitTime = tokenBucketRateLimiter.getWaitTimeForPermit();
        assertTrue("Wait time is present", waitTime.isPresent());
        long waitMillis = waitTime.getAsLong();
        assertTrue(String.format("Wait time of %dms should be between 50s and 1m", waitMillis),
                   waitMillis <= Duration.ofMinutes(1).toMillis() && waitMillis >= Duration.ofSeconds(50).toMillis());

        long now = System.currentTimeMillis();
        assertFalse(tokenBucketRateLimiter.acquirePatiently(Duration.ofMillis(500)));
        assertTrue("at least 500ms passed before the method returned, but not more than 600ms", System.currentTimeMillis() - now >= 500 && System.currentTimeMillis() - now <= 600);
    }
    @Test
    public void testRealTime() {
        // do it live
        // 10 max,
        // 10 per second refill rate
        // so ~ 40 requests in 3 seconds or ~ 13.333

        TokenBucketRateLimiter limiter = buildRateLimiter(System::currentTimeMillis, 10, 10, Duration.ofSeconds(1));
        double rate = 0.0;
        long startTime = System.nanoTime();
        for (int i = 0; i < 40; ++i) {
            assertTrue(limiter.acquirePatiently(Duration.ofMinutes(1)));
            rate++;
        }
        double meanRate = getMeanRate(rate, startTime);
        assertTrue(meanRate < 13.5 && meanRate > 10.0);

        // if we do 20 more, our bucket should be 'empty'
        // so will take about 2 more seconds, or ~12 per second

        for (int i = 0; i < 20; ++i) {
            assertTrue(limiter.acquirePatiently(Duration.ofMinutes(1)));
            rate++;
        }
        double meanRate2 = getMeanRate(rate, startTime);
        assertTrue(meanRate2 < 12.5 && meanRate2 < meanRate && meanRate2 > 10.0);
    }

    @Test
    public void testFloaty() {
        TokenBucketRateLimiter limiter = buildRateLimiter(System::currentTimeMillis, 8, 160_000, Duration.ofDays(1));
        assertTrue(limiter.acquirePatiently(Duration.ofMinutes(1)));
    }

    private double getMeanRate(double rate, long startTime) {
        if(rate == 0L) {
            return 0.0D;
        } else {
            double elapsed = (double)(System.nanoTime() - startTime);
            return rate / elapsed * (double) TimeUnit.SECONDS.toNanos(1L);
        }
    }


    private TokenBucketRateLimiter buildRateLimiter(LongSupplier clock, int maxTokens, int refillValue, Duration refillPeriod) {

        return new TokenBucketRateLimiter(
                rdbi,
                "d:test:rdbi",
                UUID.randomUUID().toString(),
                maxTokens,
                refillValue,
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