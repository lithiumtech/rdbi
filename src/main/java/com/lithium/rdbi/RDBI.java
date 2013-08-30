package com.lithium.rdbi;

import com.google.common.base.Throwables;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

public class RDBI {

    private JedisPool pool;

    public RDBI(JedisPool pool) {
        this.pool = pool;
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
        JedisHandle handle = new JedisHandle(resource);

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
}
