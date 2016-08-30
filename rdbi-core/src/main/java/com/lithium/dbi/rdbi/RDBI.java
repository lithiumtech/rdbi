package com.lithium.dbi.rdbi;

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

    final ProxyFactory proxyFactory;

    public RDBI(Pool<Jedis> pool) {
        this.pool = pool;
        this.proxyFactory = new ProxyFactory();
        logger.info("RDBI created, ready for action.");
    }

    public <T> T withHandle(Callback<T> callback) {
        try (Handle handle = open()) {
            return callback.run(handle);
        } catch (JedisException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void consumeHandle(HandleConsumer consumer) {
        try (Handle handle = open()) {
            consumer.accept(handle);
        } catch (JedisException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Handle open() {
        try {
            Jedis resource = pool.getResource();
            return new Handle(pool, resource, proxyFactory);
        } catch (Exception ex) {
            logger.error("Exception caught during resource create!", ex);
            throw new RuntimeException(ex);
        }
    }
}
