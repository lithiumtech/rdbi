package com.lithium.dbi.rdbi;

import io.opentelemetry.api.trace.Tracer;
import redis.clients.jedis.Jedis;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;

@NotThreadSafe
public class Handle implements Closeable {

    private final Jedis jedis;
    private final Tracer tracer;
    private JedisWrapperDoNotUse jedisWrapper;

    private final ProxyFactory proxyFactory;

    public Handle(Jedis jedis, ProxyFactory proxyFactory, Tracer tracer) {
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
        jedis.close();
    }
}
