package com.lithium.rdbi;

import redis.clients.jedis.Jedis;

public class JedisHandle {

    private final Jedis jedis;

    public JedisHandle(Jedis jedis) {
        this.jedis = jedis;
    }

    public Jedis jedis() {
        return jedis;
    }

    public <T> T attach(Class<T> type) {
        return RDBIProxyFactory.attach(jedis, type);
    }
}
