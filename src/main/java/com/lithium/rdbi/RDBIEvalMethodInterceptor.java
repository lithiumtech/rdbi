package com.lithium.rdbi;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

class RDBIEvalMethodInterceptor implements MethodInterceptor {

    private final Jedis jedis;
    private final Map<Method, String> shaCache;

    RDBIEvalMethodInterceptor(Jedis jedis, Map<Method, String> shaCache) {
        this.jedis = jedis;
        this.shaCache = shaCache;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        String sha1s = shaCache.get(method);
        return jedis.evalsha(sha1s, (List<String>) objects[0], (List<String>) objects[1]);
    }
}
