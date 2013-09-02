package com.lithium.rdbi;


import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.testng.Assert.assertEquals;

public class RDBIWithHandleTest {

    static interface TestDAO {
        @RedisQuery(
                "redis.call('SET',  KEYS[1], ARGV[1]);" +
                        "return 0;"
        )
        int testExec(List<String> keys, List<String> args);
    }

    @Test
    public void testBasicWithHandle() {
        RDBI rdbi = new RDBI(RDBITest.getJedisPool());

        rdbi.withHandle(new JedisCallback<Object>() {
            @Override
            public Object run(JedisHandle handle) {
                assertEquals(handle.attach(TestDAO.class).testExec(ImmutableList.of("hello"), ImmutableList.of("world")), 0);
                return null;
            }
        });
    }

    @Test
    void testBasicWithHandleFailure() {
        RDBI rdbi = new RDBI(RDBITest.getBadJedisPool());

        try {
            rdbi.withHandle(new JedisCallback<Object>() {
                @Override
                public Object run(JedisHandle handle) {
                    handle.attach(TestDAO.class);
                    return null;
                }
            });
        } catch (RuntimeException e) {
            assertFalse(rdbi.proxyFactory.methodContextCache.containsKey(TestDAO.class));
            assertFalse(rdbi.proxyFactory.factoryCache.containsKey(TestDAO.class));
        }
    }

    @Test(expectedExceptions = Exception.class)
    void testBasicWithRuntimeException() {

        RDBI rdbi = new RDBI(RDBITest.getBadJedisPool());

        rdbi.withHandle(new JedisCallback<Object>() {
            @Override
            public Object run(JedisHandle handle) {
                handle.jedis().get("hello");
                fail("Should have thrown exception on get");
                return null;
            }
        });
    }
}
