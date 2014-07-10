package com.lithium.dbi.rdbi;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Use this class as a manager for jedis and its pool as well as redis lua script loading
 */
@ThreadSafe
public class RDBI {

    private final Pool<Jedis> pool;
    private static final Logger logger = LoggerFactory.getLogger(RDBI.class);

    @VisibleForTesting
    final ProxyFactory proxyFactory;

    public RDBI(Pool<Jedis> pool) {
        this.pool = pool;
        this.proxyFactory = new ProxyFactory();
        logger.info("RDBI created, ready for action.");
    }

    public <T> T withHandle(Callback<T> callback) {

        Handle handle = open();
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

    public Handle open() {
        try {
            Jedis resource = pool.getResource();
            return new Handle(pool, resource, proxyFactory);
        } catch (Exception ex) {
            logger.error("Exception caught during resource create!", ex);
            throw Throwables.propagate(ex);
        }
    }
}
