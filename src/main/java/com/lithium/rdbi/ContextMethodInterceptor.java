package com.lithium.rdbi;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

class ContextMethodInterceptor implements MethodInterceptor {

    private final Jedis jedis;
    private final Map<Method, MethodContext> contexts;

    ContextMethodInterceptor(Jedis jedis, Map<Method, MethodContext> contexts) {
        this.jedis = jedis;
        this.contexts = contexts;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        String sha1s = contexts.get(method).getSHA1();

        Object ret =  jedis.evalsha(sha1s, (List<String>) objects[0], (List<String>) objects[1]);
        if (contexts.get(method).getMapper() != null) {
            return contexts.get(method).getMapper().map(ret);
        } else {
            return ret;
        }
    }
}
