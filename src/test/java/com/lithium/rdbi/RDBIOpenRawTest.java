package com.lithium.rdbi;

import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;

import static org.testng.Assert.assertEquals;

public class RDBIOpenRawTest {

    @Test
    public void testNormalOpen() {

        RDBI rdbi = new RDBI(RDBITest.getJedisPool());

        JedisHandle jedis = rdbi.open();

        try {
            String ret = jedis.jedis().get("hello");
            assertEquals(ret, "world");
        } finally {
            jedis.close();
        }
    }
}
