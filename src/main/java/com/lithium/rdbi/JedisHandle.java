package com.lithium.rdbi;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;

@NotThreadSafe
public class JedisHandle implements Closeable {

    private final JedisPool pool;
    private final Jedis jedis;
    private JedisWrapperDoNotUse jedisWrapper;
    private boolean closed = false;

    private final ProxyFactory proxyFactory;

    public JedisHandle(JedisPool pool, Jedis jedis, ProxyFactory proxyFactory) {
        this.pool = pool;
        this.jedis = jedis;
        this.proxyFactory = proxyFactory;
    }

    public Jedis jedis() {

        if (jedisWrapper == null) {
            jedisWrapper = proxyFactory.attachJedis(jedis);
        }

        return jedisWrapper;
    }

    public <T> T attach(Class<T> type) {
        return proxyFactory.attach(jedis(), type);
    }

    @Override
    public void close() {

        if (closed) {
            return;
        }

        try {
            if (jedisWrapper.__rdbi_isJedisBusted__()) {
                pool.returnBrokenResource(jedis);
            } else {
                pool.returnResource(jedis);
            }
        } catch (ClassCastException e) {
            //internal error
        } finally {
            //for indempotency of Closeable interface
            closed = true;
        }
    }
}
