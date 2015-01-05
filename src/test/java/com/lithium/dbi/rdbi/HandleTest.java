package com.lithium.dbi.rdbi;

import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HandleTest {
    @Test
    public void testReturnBrokenResource() {
        final Pool<Jedis> fakePool = mock(Pool.class);
        doThrow(new JedisConnectionException("boogaboogahey")).when(fakePool).returnResource(any(Jedis.class));
        final Jedis fakeJedis = mock(Jedis.class);
        final JedisWrapperDoNotUse fakeWrapper = mock(JedisWrapperDoNotUse.class);
        when(fakeWrapper.__rdbi_isJedisBusted__()).thenReturn(false);

        // mocks that return mocks.... forgive me....
        final ProxyFactory fakeProxyFactory = mock(ProxyFactory.class);
        when(fakeProxyFactory.attachJedis(any(Jedis.class))).thenReturn(fakeWrapper);

        final Handle testHandle = new Handle(fakePool, fakeJedis, fakeProxyFactory);
        testHandle.close();

        verify(fakePool).returnResource(fakeJedis);
        verify(fakePool).returnBrokenResource(fakeJedis);
    }
}
