package com.lithium.dbi.rdbi;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class RDBIOpenRawTest {

    @Test
    public void testNormalOpen() {

        RDBI rdbi = new RDBI(RDBITest.getJedisPool());

        Handle jedis = rdbi.open();

        try {
            String ret = jedis.jedis().get("hello");
            assertEquals(ret, "world");
        } finally {
            jedis.close();
        }
    }
}
