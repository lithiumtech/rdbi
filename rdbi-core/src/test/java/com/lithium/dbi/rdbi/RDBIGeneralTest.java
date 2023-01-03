package com.lithium.dbi.rdbi;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import static org.testng.Assert.assertEquals;

/**
 * User: phutwo
 * Date: 9/8/13
 * Time: 12:00 AM
 */
public class RDBIGeneralTest {

    public static void main(String[] args) {
        RDBI rdbi = new RDBI(new JedisPool("localhost", 6379));

        try (Handle handle = rdbi.open()) {
            Jedis jedis = handle.jedis();
            jedis.set("hey", "now");
        }

        try (Handle handle = rdbi.open()) {
            Jedis jedis = handle.jedis();
            String hey = jedis.get("hey");
            assertEquals(hey, "now");
        }
    }
}
