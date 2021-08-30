package com.lithium.dbi.rdbi;

import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;

@NotThreadSafe
public class Handle implements Closeable {

    private final Pool<Jedis> pool;
    private final Jedis jedis;
    private final Tracer tracer;
    private JedisWrapperDoNotUse jedisWrapper;
    private boolean closed = false;
    private static final Logger logger = LoggerFactory.getLogger(Handle.class);

    private final ProxyFactory proxyFactory;

    public Handle(Pool<Jedis> pool, Jedis jedis, ProxyFactory proxyFactory, Tracer tracer) {
        this.pool = pool;
        this.jedis = jedis;
        this.proxyFactory = proxyFactory;
        this.tracer = tracer;
    }

    public Jedis jedis() {

        if (jedisWrapper == null) {
            jedisWrapper = proxyFactory.attachJedis(jedis, tracer);
        }

        return jedisWrapper;
    }

    public <T> T attach(Class<T> type) {
        return proxyFactory.createInstance(jedis(), type);
    }

    @Override
    public void close() {

        if (closed) {
            return;
        }

        boolean isBusted;
        try {
            if (jedisWrapper != null) {
                isBusted = jedisWrapper.__rdbi_isJedisBusted__();
            } else {
                isBusted = false;
            }
        } catch (Exception e) {
            logger.error("Exception caught while checking isJedisBusted!", e);
            isBusted = true;
        }

        if (isBusted) {
            pool.returnBrokenResource(jedis);
        } else {
            try {
                pool.returnResource(jedis);
            } catch (Exception ex) {
                logger.error("Exception caught while trying to return to pool. Returning as broken.", ex);
                pool.returnBrokenResource(jedis);
            }
        }
        closed = true;
    }
}
