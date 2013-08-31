package com.lithium.rdbi;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisHandle {

    private final JedisPool pool;
    private final Jedis jedis;
    private final ProxyFactory proxyFactory;

    public JedisHandle(JedisPool pool, Jedis jedis, ProxyFactory proxyFactory) {
        this.pool = pool;
        this.jedis = jedis;
        this.proxyFactory = proxyFactory;
    }

    public Jedis jedis() {
        return jedis;
    }

    public <T> T attach(Class<T> type) {
        return proxyFactory.attach(pool, jedis, type);
    }
}
