package com.lithium.dbi.rdbi;

import redis.clients.jedis.Jedis;

abstract class JedisWrapperDoNotUse extends Jedis {

    JedisWrapperDoNotUse() {
        super("doesntmatter");
    }

    abstract boolean __rdbi_isJedisBusted__();
}
