package com.lithium.dbi.rdbi;


import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

public class RDBIWithHandleTest {

    static interface TestDAO {
        @Query(
                "redis.call('SET',  KEYS[1], ARGV[1]);" +
                        "return 0;"
        )
        int testExec(List<String> keys, List<String> args);
    }

    @Test
    public void testBasicWithHandle() {
        RDBI rdbi = new RDBI(RDBITest.getMockJedisPool());

        rdbi.withHandle(new Callback<Object>() {
            @Override
            public Object run(Handle handle) {
                assertEquals(handle.attach(TestDAO.class).testExec(Collections.singletonList("hello"), Collections.singletonList("world")), 0);
                return null;
            }
        });
    }

    @Test
    void testBasicWithHandleFailure() {
        RDBI rdbi = new RDBI(RDBITest.getBadJedisPool());

        try {
            rdbi.withHandle(new Callback<Object>() {
                @Override
                public Object run(Handle handle) {
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

        rdbi.withHandle(new Callback<Object>() {
            @Override
            public Object run(Handle handle) {
                handle.jedis().get("hello");
                fail("Should have thrown exception on get");
                return null;
            }
        });
    }
}
