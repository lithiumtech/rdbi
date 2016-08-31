package com.lithium.dbi.rdbi;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * User: phutwo
 * Date: 9/8/13
 * Time: 12:00 AM
 */
public class RDBIGeneralTest {

    public static void main(String[] args) {
        RDBI rdbi = new RDBI(new JedisPool("localhost"));

        Handle handle = rdbi.open();

        try {
            Jedis jedis = handle.jedis();
        } finally {
            handle.close();
        }
    }
}
