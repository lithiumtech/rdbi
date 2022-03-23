package com.lithium.dbi.rdbi;

import io.opentelemetry.api.GlobalOpenTelemetry;
import org.apache.commons.pool2.impl.EvictionPolicy;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.util.Pool;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class HandleTest {

    Logger logger = LoggerFactory.getLogger(HandleTest.class);

    @Test
    public void testReturnBrokenResource() {
        final Pool<Jedis> fakePool = mock(Pool.class);
        doThrow(new JedisConnectionException("boogaboogahey")).when(fakePool).returnResource(any(Jedis.class));
        final Jedis fakeJedis = mock(Jedis.class);
        final JedisWrapperDoNotUse fakeWrapper = mock(JedisWrapperDoNotUse.class);

        // mocks that return mocks.... forgive me....
        final ProxyFactory fakeProxyFactory = mock(ProxyFactory.class);
        when(fakeProxyFactory.attachJedis(any(Jedis.class), any())).thenReturn(fakeWrapper);

        final Handle testHandle = new Handle(fakeJedis, fakeProxyFactory, GlobalOpenTelemetry.getTracer(RDBI.TRACER_NAME));
        testHandle.close();

        verify(fakePool).returnResource(fakeJedis);
        verify(fakePool).returnBrokenResource(fakeJedis);
    }

    @Test
    public void evictResourceTestRdbi() throws InterruptedException {
        final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        jedisPoolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
        jedisPoolConfig.setMinEvictableIdleTime(Duration.ofSeconds(1));
        jedisPoolConfig.setEvictionPolicy(chattyDefaultEvictionPolicy());

        JedisPool pool = new JedisPool(jedisPoolConfig,
                                       "localhost", 6379, 1000);
        RDBI rdbi = new RDBI(pool);

        String val = rdbi.withHandle(h -> {
            h.jedis().set("key", "value");
            logger.info("jedis1: {}",      System.identityHashCode(h.jedis()) );
            logger.info("jedis1 chc: {}",    System.identityHashCode(h.jedis().getConnection())   );

            return h.jedis().get("key");
        });
        rdbi.withHandle(h -> {
            h.jedis().set("key", "value");
            logger.info("jedis2: {}",       System.identityHashCode(h.jedis()));
            logger.info("jedis2 chc: {}",   System.identityHashCode(h.jedis().getConnection()));

            return h.jedis().get("key");
        });
         rdbi.withHandle(h -> {
            h.jedis().set("key", "value");
             logger.info("jedis3: {}",       System.identityHashCode(h.jedis()));
             logger.info("jedis3 chc: {}",   System.identityHashCode(h.jedis().getConnection()));
             return h.jedis().get("key");
        });
        assertEquals(val, "value");

        // wait.
        Thread.sleep(30_000);

    }


    @Test
    public void evictResourceTestJedisOnly() throws InterruptedException {
        final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        jedisPoolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(5));
        jedisPoolConfig.setMinEvictableIdleTime(Duration.ofSeconds(10));
        jedisPoolConfig.setEvictionPolicy(chattyDefaultEvictionPolicy());

        JedisPool pool = new JedisPool(jedisPoolConfig,
                                       "localhost", 6379, 1000);
        try (Jedis jedis = pool.getResource()) {
            jedis.set("key", "value");
            assertEquals(jedis.get("key"), "value");
        }


        try (Jedis jedis = pool.getResource()) {
            jedis.set("key", "value");
            assertEquals(jedis.get("key"), "value");
        }

        // wait.
        Thread.sleep(30_000);

    }

    @Test
    public void evictResourceTestPooledJedisOnly() throws InterruptedException {
        final GenericObjectPoolConfig<Connection> jedisPoolConfig = new GenericObjectPoolConfig<Connection>();

        jedisPoolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(5));
        jedisPoolConfig.setMinEvictableIdleTime(Duration.ofSeconds(10));
        jedisPoolConfig.setEvictionPolicy(chattyDefaultEvictionPolicy());


        JedisPooled jedisPooled = new JedisPooled(jedisPoolConfig, "localhost", 6379);

        jedisPooled.set("key", "value");
        assertEquals(jedisPooled.get("key"), "value");


        // wait.
        Thread.sleep(30_000);

    }

    private <T> EvictionPolicy<T> chattyDefaultEvictionPolicy() {
        return (config, underTest, idleCount) -> {
            logger.info("testing eviction on {} @ {}", underTest, underTest.hashCode());
            boolean evict = (config.getIdleSoftEvictDuration().compareTo(underTest.getIdleDuration()) < 0 &&
                    config.getMinIdle() < idleCount) ||
                    config.getIdleEvictDuration().compareTo(underTest.getIdleDuration()) < 0;
            logger.info("idle duration compare = {}", config.getIdleEvictDuration().compareTo(underTest.getIdleDuration()));
            logger.info("default eviction says: {}", evict);
            return evict;
        };
    }
}
