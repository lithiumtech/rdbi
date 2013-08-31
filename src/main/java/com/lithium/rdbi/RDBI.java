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

    private JedisPool pool;

    @VisibleForTesting
    ProxyFactory proxyFactory;

    public RDBI(JedisPool pool) {
        this.pool = pool;
        this.proxyFactory = new ProxyFactory();
    }

    /**
     * Attaches a jedis handle and run with handle. RDBI will take care of the resource cleanup and exception propagation
     *
     * @param callback The callback instance
     * @param <T> the class type the callback will return
     * @return the value the callback class will return
     */
    public <T> T withHandle(JedisCallback<T> callback) {
        Jedis resource = pool.getResource();
        JedisHandle handle = new JedisHandle(pool, resource, proxyFactory);

        try {
            T result = callback.run(handle);
            pool.returnResource(resource);
            return result;
        } catch (JedisException e) {
            pool.returnBrokenResource(resource);
            throw Throwables.propagate(e);
        } catch (Exception e) {
            pool.returnBrokenResource(resource);
            throw Throwables.propagate(e);
        }
    }

    public <T> T open(Class<T> t) {
        Jedis resource = pool.getResource();

        try {
            return proxyFactory.attach(pool, resource, t);
        } catch (JedisException e) {
            pool.returnBrokenResource(resource);
            throw Throwables.propagate(e);
        } catch (Exception e) {
            pool.returnBrokenResource(resource);
            throw Throwables.propagate(e);
        }
    }

    public Jedis getJedis() {
        return pool.getResource();
    }

    public void returnBrokenJedis(Jedis jedis) {
        pool.returnBrokenResource(jedis);
    }

    public void returnJedis(Jedis jedis) {
        pool.returnBrokenResource(jedis);
    }

    public void closeBroken(Object t) {
        RDBIClosable rdbiClosable = (RDBIClosable) t;
        rdbiClosable.__rdbi__close_broken__();
    }

    public void close(Object t) {
        RDBIClosable rdbiClosable = (RDBIClosable) t;
        rdbiClosable.__rdbi__close__();
    }
}
