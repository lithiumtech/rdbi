package com.lithium.dbi.rdbi.recipes.cache;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.Callable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("unchecked")
@Test(groups = { "integration" })
public class SimpleRedisCacheTest {

    private static final String TEST_KEY = "junit:test:object";
    private static final TypeReference<String> TYPE_STRING = new TypeReference<String>(){};
    private static final TypeReference<FooBar> TYPE_FOOBAR = new TypeReference<FooBar>(){};

    private RDBI rdbi;
    private SimpleMultiCacheRedis simpleCache;
    private CacheMetrics cacheMetrics;

    @BeforeMethod
    public void setup() {
        if (rdbi == null) {
            this.rdbi = new RDBI(new JedisPool("localhost"));
        }
        this.simpleCache = new SimpleMultiCacheRedis(new ObjectMapper(), rdbi);
        this.cacheMetrics = new CacheMetrics(new MetricRegistry(), SimpleRedisCacheTest.class, "integrationTest");
        try (Handle h = rdbi.open()) {
            h.jedis().del(TEST_KEY);
        }
    }

    @Test
    public void stringCacheTest() throws Exception {
        Callable<String> mockSupplier = mock(Callable.class);
        when(mockSupplier.call()).thenReturn("testing-1-2-3");
        String result1 = simpleCache.loadWithFallback(TEST_KEY, TYPE_STRING, mockSupplier, 60, cacheMetrics);
        String result2 = simpleCache.loadWithFallback(TEST_KEY, TYPE_STRING, mockSupplier, 60, cacheMetrics);
        assertEquals("testing-1-2-3", result1);
        assertEquals("testing-1-2-3", result2);
        verify(mockSupplier, times(1)).call();
        assertEquals(1, cacheMetrics.countHits());
        assertEquals(1, cacheMetrics.countMisses());
    }

    @Test
    public void fooBarCacheTest() throws Exception {
        Callable<FooBar> mockSupplier = mock(Callable.class);
        when(mockSupplier.call()).thenReturn(new FooBar("hello", 123));
        FooBar result1 = simpleCache.loadWithFallback(TEST_KEY, TYPE_FOOBAR, mockSupplier, 60, cacheMetrics);
        FooBar result2 = simpleCache.loadWithFallback(TEST_KEY, TYPE_FOOBAR, mockSupplier, 60, cacheMetrics);
        assertEquals("hello", result1.getFoo());
        assertEquals(123, result1.getBar());
        assertEquals("hello", result2.getFoo());
        assertEquals(123, result2.getBar());
        verify(mockSupplier, times(1)).call();
        assertEquals(1, cacheMetrics.countHits());
        assertEquals(1, cacheMetrics.countMisses());
    }

    @Test
    public void nullMetrics() throws Exception {
        Callable<FooBar> mockSupplier = mock(Callable.class);
        when(mockSupplier.call()).thenReturn(new FooBar("hello", 123));
        FooBar result1 = simpleCache.loadWithFallback(TEST_KEY, TYPE_FOOBAR, mockSupplier);
        FooBar result2 = simpleCache.loadWithFallback(TEST_KEY, TYPE_FOOBAR, mockSupplier);
        assertEquals("hello", result1.getFoo());
        assertEquals(123, result1.getBar());
        assertEquals("hello", result2.getFoo());
        assertEquals(123, result2.getBar());
        verify(mockSupplier, times(1)).call();
        assertEquals(0, cacheMetrics.countHits());
        assertEquals(0, cacheMetrics.countMisses());
    }

    private static class FooBar {
        private String foo;
        private int bar;

        public FooBar(String foo, int bar) {
            this.foo = foo;
            this.bar = bar;
        }
        public FooBar() {}

        public String getFoo() {
            return foo;
        }
        public void setFoo(String foo) {
            this.foo = foo;
        }
        public int getBar() {
            return bar;
        }
        public void setBar(int bar) {
            this.bar = bar;
        }
    }
}
