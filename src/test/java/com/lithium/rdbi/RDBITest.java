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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class RDBITest {

    static interface TestDAO {
        @RedisQuery(
            "redis.call('SET',  KEYS[1], ARGV[1]);" +
            "return 0;"
        )
        int testExec(List<String> keys, List<String> args);
    }

    static interface TestCopyDAO {
        @RedisQuery(
                "redis.call('SET',  KEYS[1], ARGV[1]);" +
                        "return 0;"
        )
        int testExec2(List<String> keys, List<String> args);
    }

    static interface NoInputDAO {
        @RedisQuery("return 0;")
        int noInputMethod();
    }

    static interface DynamicDAO {
        @RedisQuery(
                "redis.call('SET', $a$, $b$); return 0;"
        )
        int testExect(@BindKey("a") String a, @Bind("b") String b);
    }

    static class BasicObjectUnderTest {

        private final String input;

        public BasicObjectUnderTest(Integer input) {
            this.input = input.toString();
        }

        public String getInput() {
            return input;
        }
    }
    static class BasicRedisResultMapper implements RedisResultMapper<BasicObjectUnderTest> {
        @Override
        public BasicObjectUnderTest map(Object result) {
            return new BasicObjectUnderTest((Integer) result);
        }
    }
    static interface TestDAOWithResultSetMapper {

        @RedisQuery(
            "redis.call('SET',  KEYS[1], ARGV[1]);" +
            "return 0;"
        )
        @Mapper(BasicRedisResultMapper.class)
        BasicObjectUnderTest testExec(List<String> keys, List<String> args);
    }

    @Test
    public void testExceptionThrownInRDBIAttach() {
        RDBI rdbi = new RDBI(getBadJedisPool());

        try {
            rdbi.withHandle(new JedisCallback<Void>() {
                @Override
                public Void run(JedisHandle handle) {
                    handle.attach(TestCopyDAO.class);
                    return null;
                }
            });
            fail("Should have thrown exception for loadScript error");
        } catch (RuntimeException e) {
            //expected
            assertFalse(rdbi.proxyFactory.factoryCache.containsKey(TestCopyDAO.class));
            assertFalse(rdbi.proxyFactory.methodContextCache.containsKey(TestCopyDAO.class));
        }
    }

    @Test
    public void testExceptionThrownInNormalGet() {
        RDBI rdbi = new RDBI(getBadJedisPool());

        try {
            rdbi.withHandle(new JedisCallback<Void>() {
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

        rdbi.withHandle(new JedisCallback<Object>() {
            @Override
            public Object run(JedisHandle handle) {
                assertEquals(handle.attach(TestDAO.class).testExec(ImmutableList.of("hello"), ImmutableList.of("world")), 0);
                return null;
            }
        });
        rdbi.withHandle(new JedisCallback<Void>() {
            @Override
            public Void run(JedisHandle handle) {
                assertEquals(handle.attach(TestDAO.class).testExec(ImmutableList.of("hello"), ImmutableList.of("world")), 0);
                return null;
            }
        });

        assertTrue(rdbi.proxyFactory.factoryCache.containsKey(TestDAO.class));
        assertTrue(rdbi.proxyFactory.methodContextCache.containsKey(TestDAO.class));

        rdbi.withHandle(new JedisCallback<Object>() {
            @Override
            public Object run(JedisHandle handle) {
                String result = handle.jedis().get("hello");
                assertEquals("world", result);
                return null;
            }
        });
    }

    @Test
    public void testAttachWithResultSetMapper() {
        RDBI rdbi = new RDBI(getJedisPool());
        rdbi.withHandle(new JedisCallback<Object>() {
            @Override
            public Object run(JedisHandle handle) {
                BasicObjectUnderTest dut = handle.attach(TestDAOWithResultSetMapper.class).testExec(ImmutableList.of("hello"), ImmutableList.of("world"));
                assertNotNull(dut);
                assertEquals(dut.getInput(), "0");
                return null;
            }
        });
    }

    @Test
    public void testMethodWithNoInput() {
        RDBI rdbi = new RDBI(getJedisPool());
        rdbi.withHandle(new JedisCallback<Object>() {
            @Override
            public Object run(JedisHandle handle) {
                int ret = handle.attach(NoInputDAO.class).noInputMethod();
                assertEquals(ret, 0);
                return null;
            }
        });
    }

    @Test
    public void testDynamicDAO() {
        RDBI rdbi = new RDBI(getJedisPool());

        rdbi.withHandle(new JedisCallback<Object>() {
            @Override
            public Object run(JedisHandle handle) {

                handle.attach(DynamicDAO.class).testExect("a", "b");
                return null;
            }
        });
    }

    @Test
    public void testCacheHitDAO() {
        RDBI rdbi = new RDBI(getJedisPool());

        for (int i = 0; i < 2; i++) {
            rdbi.withHandle(new JedisCallback<Object>() {
                @Override
                public Object run(JedisHandle handle) {
                    handle.attach(DynamicDAO.class).testExect("a", "b");
                    return null;
                }
            });
            assertTrue(rdbi.proxyFactory.factoryCache.containsKey(DynamicDAO.class));
        }
    }

    @SuppressWarnings("unchecked")
    static JedisPool getJedisPool() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.scriptLoad(anyString())).thenReturn("my-sha1-hash");
        when(jedis.evalsha(anyString(), anyList(), anyList())).thenReturn(0);
        when(jedis.get("hello")).thenReturn("world");

        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenReturn(jedis);
        return pool;
    }

    static JedisPool getBadJedisPool() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.scriptLoad(anyString())).thenThrow(new JedisException("thrown in test"));
        when(jedis.get("hello")).thenThrow(new RuntimeException("runtime exception"));

        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenReturn(jedis);
        return pool;
    }

    static JedisPool getBadJedisPoolWithRuntimeException() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.scriptLoad(anyString())).thenThrow(new RuntimeException("thrown in test"));
        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenReturn(jedis);
        return pool;
    }
}
