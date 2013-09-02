package com.lithium.rdbi;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.lang.reflect.Method;

class JedisWrapperMethodInterceptor implements MethodInterceptor {

    private final Jedis jedis;
    private boolean jedisBusted;

    static Factory newFactory() {
        Enhancer e = new Enhancer();
        e.setSuperclass(JedisWrapper.class);
        e.setCallback(new MethodNoOpInterceptor());
        return (Factory) e.create();
    }

    static JedisWrapper newInstance(final Factory factory, final Jedis realJedis) {
        return (JedisWrapper) factory.newInstance(new JedisWrapperMethodInterceptor(realJedis));
    }

    private JedisWrapperMethodInterceptor(Jedis jedis) {
        this.jedis = jedis;
        jedisBusted = false;
    }

    @Override
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        try {

            if ( method.equals("__rdbi_isJedisBusted__")) {
                return jedisBusted;
            }

            return methodProxy.invoke(jedis, objects);
        } catch (JedisException e) {
            jedisBusted = true;
            throw e;
        }
    }
}
