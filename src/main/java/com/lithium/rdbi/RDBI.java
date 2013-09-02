package com.lithium.rdbi;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Use this class as a manager for jedis and its pool as well as redis lua script loading
 */
@ThreadSafe
public class RDBI {

    private final JedisPool pool;

    @VisibleForTesting
    final ProxyFactory proxyFactory;

    public RDBI(JedisPool pool) {
        this.pool = pool;
        this.proxyFactory = new ProxyFactory();
    }

    public <T> T withHandle(JedisCallback<T> callback) {

        JedisHandle handle = open();
        try {
            T result = callback.run(handle);
            return result;
        } catch (JedisException e) {
            throw Throwables.propagate(e);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            handle.close();
        }
    }

    public JedisHandle open() {
        Jedis resource = pool.getResource();
        return new JedisHandle(pool, resource, proxyFactory);
    }
}
