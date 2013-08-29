package com.lithium.rdbi;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RDBITest {

    static interface TestDAO2 {
        @RedisQuery(
            "redis.call('SET',  KEYS[1], ARGV[1]);" +
            "return 0;"
        )
        int testExec2(List<String> keys, List<String> args);
    }

    static interface TestDAO {
        @RedisQuery(
            "redis.call('SET',  KEYS[1], ARGV[1]);" +
            "return 0;"
        )
        int testExec(List<String> keys, List<String> args);
    }

    @Test
    public void testExceptionThrownInRDBIAttach() {
        RDBI rdbi = new RDBI(getBadJedisPool());

        try {
            rdbi.withHandle(new RDBICallback<Void>() {
                @Override
                public Void run(JedisHandle handle) {
                    handle.attach(TestDAO2.class);
                    return null;
                }
            });
            fail("Should have thrown exception for loadScript error");
        } catch (RuntimeException e) {
            //expected
            assertFalse(RDBIProxyFactory.factoryCache.containsKey(TestDAO2.class));
            assertFalse(RDBIProxyFactory.sha1Cache.containsKey(TestDAO2.class));
        }
    }

    @Test
    public void testExceptionThrownInNormalGet() {
        RDBI rdbi = new RDBI(getBadJedisPool());

        try {
            rdbi.withHandle(new RDBICallback<Void>() {
                @Override
                public Void run(JedisHandle handle) {
                    handle.jedis().get("hello");
                    return null;
                }
            });
            fail("Should have thrown exception on get");
        } catch (Exception e) {
            //expected
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBasicAttachRun() {

        RDBI rdbi = new RDBI(getJedisPool());

        rdbi.withHandle(new RDBICallback<Object>() {
            @Override
            public Object run(JedisHandle handle) {
                assertEquals(handle.attach(TestDAO.class).testExec(ImmutableList.of("hello"), ImmutableList.of("world")), 0);
                return null;
            }
        });
        rdbi.withHandle(new RDBICallback<Void>() {
            @Override
            public Void run(JedisHandle handle) {
                assertEquals(handle.attach(TestDAO.class).testExec(ImmutableList.of("hello"), ImmutableList.of("world")), 0);
                return null;
            }
        });

        assertTrue(RDBIProxyFactory.factoryCache.containsKey(TestDAO.class));
        assertTrue(RDBIProxyFactory.sha1Cache.containsKey(TestDAO.class));

        rdbi.withHandle(new RDBICallback<Object>() {
            @Override
            public Object run(JedisHandle handle) {
                String result = handle.jedis().get("hello");
                assertEquals("world", result);
                return null;
            }
        });
    }

    private JedisPool getJedisPool() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.scriptLoad(anyString())).thenReturn("my-sha1-hash");
        when(jedis.evalsha(anyString(), anyList(), anyList())).thenReturn(0);
        when(jedis.get("hello")).thenReturn("world");

        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenReturn(jedis);
        return pool;
    }

    private JedisPool getBadJedisPool() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.scriptLoad(anyString())).thenThrow(new JedisException("thrown in test"));
        when(jedis.get("hello")).thenThrow(new RuntimeException("runtime exception"));

        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenReturn(jedis);
        return pool;
    }
}
