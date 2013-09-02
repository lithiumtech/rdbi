package com.lithium.rdbi;

import redis.clients.jedis.Jedis;

public abstract class JedisWrapper extends Jedis {

    JedisWrapper() {
        super("doesntmatter");
    }

    abstract boolean __rdbi_isJedisBusted__();
}
