package com.lithium.dbi.rdbi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;

@NotThreadSafe
public class Handle implements Closeable {
    private final Jedis jedis;
    private JedisWrapperDoNotUse jedisWrapper;
    private static final Logger logger = LoggerFactory.getLogger(Handle.class);

    private final ProxyFactory proxyFactory;

    public Handle(Jedis jedis, ProxyFactory proxyFactory) {
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
        return proxyFactory.createInstance(jedis(), type);
    }

    @Override
    public void close() {
        try {
            jedis.close();
        } catch (Exception ex) {
            logger.error("error closing jedis resource", ex);
        }
    }
}
