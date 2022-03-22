package com.lithium.dbi.rdbi;

import redis.clients.jedis.Jedis;

abstract class JedisWrapperDoNotUse extends Jedis {

    JedisWrapperDoNotUse() {
        super();
    }

    abstract boolean __rdbi_isJedisBusted__();
}
