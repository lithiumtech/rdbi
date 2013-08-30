package com.lithium.rdbi;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

class ProxyFactory {

    @VisibleForTesting
    static final ConcurrentMap<Class<?>, Factory> factoryCache = Maps.newConcurrentMap();

    @VisibleForTesting
    static final ConcurrentMap<Class<?>, Map<Method, MethodContext>> methodContextCache = Maps.newConcurrentMap();

    @SuppressWarnings("unchecked")
    static <T> T attach(final Jedis jedis, final Class<T> t) {

        Factory factory;
        if (factoryCache.containsKey(t)) {
            factory = factoryCache.get(t);
        } else {

            try {
                buildMethodContext(t, jedis);
            } catch (IllegalAccessException e) {
                throw Throwables.propagate(e);
            } catch (InstantiationException e) {
                throw Throwables.propagate(e);
            }

            Enhancer e = new Enhancer();
            e.setSuperclass(t);
            e.setCallback(new MethodInterceptor() {
                @Override
                public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
                    return null;
                }
            });

            factory = (Factory) e.create();
            factoryCache.putIfAbsent(t, factory);
        }
        return (T) factory.newInstance(new ContextMethodInterceptor(jedis, methodContextCache.get(t)));
    }

    private static <T> void buildMethodContext(Class<T> t, Jedis jedis) throws IllegalAccessException, InstantiationException {

        if (methodContextCache.containsKey(t)) {
            return;
        }

        Map<Method, MethodContext> contexts = Maps.newHashMap();

        for (Method method : t.getDeclaredMethods()) {

            RedisQuery redisQuery = method.getAnnotation(RedisQuery.class);
            String queryStr = redisQuery.value();
            method.getParameterAnnotations();
            String sha1 = jedis.scriptLoad(queryStr);

            Mapper methodMapper = method.getAnnotation(Mapper.class);
            RedisResultMapper mapper = null;
            if (methodMapper != null) {
                mapper = methodMapper.value().newInstance();
            }

            contexts.put(method, new MethodContext(sha1, mapper));
        }

        methodContextCache.putIfAbsent(t, contexts);
    }
}
