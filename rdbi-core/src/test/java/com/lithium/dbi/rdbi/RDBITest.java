package com.lithium.dbi.rdbi;

import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class RDBITest {

    public interface TestDAO {
        @Query(
            "redis.call('SET',  KEYS[1], ARGV[1]);" +
            "return 0;"
        )
        int testExec(List<String> keys, List<String> args);
    }

    public interface TestCopyDAO {
        @Query(
                "redis.call('SET',  KEYS[1], ARGV[1]);" +
                        "return 0;"
        )
        int testExec2(List<String> keys, List<String> args);
    }

    public interface NoInputDAO {
        @Query("return 0;")
        int noInputMethod();
    }

    public interface DynamicDAO {
        @Query(
                "redis.call('SET', $a$, $b$); return 0;"
        )
        int testExec(@BindKey("a") String a, @BindArg("b") String b);
    }

    public static class BasicObjectUnderTest {

        private final String input;

        public BasicObjectUnderTest(Integer input) {
            this.input = input.toString();
        }

        public String getInput() {
            return input;
        }
    }
    public static class BasicResultMapper implements ResultMapper<BasicObjectUnderTest, Integer> {
        @Override
        public BasicObjectUnderTest map(Integer result) {
            return new BasicObjectUnderTest(result);
        }
    }
    public interface TestDAOWithResultSetMapper {

        @Query(
            "redis.call('SET',  KEYS[1], ARGV[1]);" +
            "return 0;"
        )
        @Mapper(BasicResultMapper.class)
        BasicObjectUnderTest testExec(List<String> keys, List<String> args);
    }

    @Test
    public void testExceptionThrownInRDBIAttach() {
        RDBI rdbi = new RDBI(getBadJedisPool());

        Handle handle = rdbi.open();
        try {
            handle.attach(TestCopyDAO.class);
            fail("Should have thrown exception for loadScript error");
        } catch (RuntimeException e) {
            //expected
            assertFalse(rdbi.proxyFactory.isCached(TestCopyDAO.class));
        } finally {
            handle.close();
        }
    }

    @Test
    public void testExceptionThrownInNormalGet() {
        RDBI rdbi = new RDBI(getBadJedisPool());

        Handle handle = rdbi.open();
        try {
            handle.jedis().get("hello");
            fail("Should have thrown exception on get");
        } catch (Exception e) {
            //expected // hmm i don't think this is right
        } finally {
            handle.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBasicAttachRun() {

        RDBI rdbi = new RDBI(getMockJedisPool());

        Handle handle1 = rdbi.open();
        try {
            assertEquals(handle1.attach(TestDAO.class).testExec(Collections.singletonList("hello"), Collections.singletonList("world")), 0);
        } finally {
            handle1.close();
        }

        Handle handle2 = rdbi.open();
        try {
            assertEquals(handle2.attach(TestDAO.class).testExec(Collections.singletonList("hello"), Collections.singletonList("world")), 0);
        } finally {
            handle2.close();
        }

        assertTrue(rdbi.proxyFactory.isCached(TestDAO.class));

        Handle handle3 = rdbi.open();
        try {
            assertEquals("world", handle3.jedis().get("hello"));
        } finally {
            handle3.close();
        }
    }

    @Test
    public void testAttachWithResultSetMapper() {
        RDBI rdbi = new RDBI(getMockJedisPool());

        Handle handle = rdbi.open();
        try {
            BasicObjectUnderTest dut = handle.attach(TestDAOWithResultSetMapper.class).testExec(Collections.singletonList("hello"), Collections.singletonList("world"));
            assertNotNull(dut);
            assertEquals(dut.getInput(), "0");
        } finally {
            handle.close();
        }
    }

    @Test
    public void testMethodWithNoInput() {
        RDBI rdbi = new RDBI(getMockJedisPool());

        Handle handle = rdbi.open();
        try {
            int ret = handle.attach(NoInputDAO.class).noInputMethod();
            assertEquals(ret, 0);
        } finally {
            handle.close();
        }
    }

    @Test
    public void testDynamicDAO() {
        RDBI rdbi = new RDBI(getMockJedisPool());

        try (Handle handle = rdbi.open()) {
            handle.attach(DynamicDAO.class).testExec("a", "b");
        }
    }

    @Test
    public void testCacheHitDAO() {
        RDBI rdbi = new RDBI(getMockJedisPool());
        Handle handle = rdbi.open();

        try {
            for (int i = 0; i < 2; i++) {
                handle.attach(DynamicDAO.class).testExec("a", "b");
            }
            assertTrue(rdbi.proxyFactory.isCached(DynamicDAO.class));
        } finally {
            handle.close();
        }
    }

    @SuppressWarnings("unchecked")
    static JedisPool getMockJedisPool() {
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
