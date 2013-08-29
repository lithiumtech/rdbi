package com.lithium.rdbi;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

class RDBIProxyFactory {

    @VisibleForTesting
    static final ConcurrentMap<Class<?>, Factory> factoryCache = Maps.newConcurrentMap();

    @VisibleForTesting
    static final ConcurrentMap<Class<?>, Map<Method, String>> sha1Cache = Maps.newConcurrentMap();

    @SuppressWarnings("unchecked")
    static <T> T attach(final Jedis jedis, final Class<T> t) {

        Factory factory;
        if (factoryCache.containsKey(t)) {
            factory = factoryCache.get(t);
        } else {
            buildSHA1s(t, jedis);
            Enhancer e = new Enhancer();
            e.setSuperclass(t);
            e.setCallback(new RDBIMethodInterceptor());

            factory = (Factory) e.create();
            factoryCache.putIfAbsent(t, factory);
        }
        return (T) factory.newInstance(new RDBIEvalMethodInterceptor(jedis, sha1Cache.get(t)));
    }

    private static <T> Map<Method, String> buildSHA1s(Class<T> t, Jedis jedis) {

        if (sha1Cache.containsKey(t)) {
            return sha1Cache.get(t);
        }

        Map<Method, String> sha1s = Maps.newHashMap();
        for (Method method : t.getDeclaredMethods()) {
            RedisQuery redisQuery = method.getAnnotation(RedisQuery.class);
            String queryStr = redisQuery.value();
            sha1s.put(method, jedis.scriptLoad(queryStr));
        }

        sha1Cache.putIfAbsent(t, sha1s);
        return sha1s;
    }
}
