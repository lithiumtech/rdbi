package com.lithium.rdbi;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

class ProxyFactory {

    @VisibleForTesting
    final ConcurrentMap<Class<?>, Factory> factoryCache = Maps.newConcurrentMap();

    @VisibleForTesting
    final ConcurrentMap<Class<?>, Map<Method, MethodContext>> methodContextCache = Maps.newConcurrentMap();

    @SuppressWarnings("unchecked")
    <T> T attach(final JedisPool pool, final Jedis jedis, final Class<T> t) {

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
            e.setInterfaces(new Class[]{t, RDBIClosable.class});
            e.setCallback(new MethodInterceptor() {
                @Override
                public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
                    return null;
                }
            });

            factory = (Factory) e.create();
            factoryCache.putIfAbsent(t, factory);
        }
        return (T) factory.newInstance(new MethodContextInterceptor(pool, jedis, methodContextCache.get(t)));
    }

    private <T> void buildMethodContext(Class<T> t, Jedis jedis) throws IllegalAccessException, InstantiationException {

        if (methodContextCache.containsKey(t)) {
            return;
        }

        Map<Method, MethodContext> contexts = Maps.newHashMap();

        for (Method method : t.getDeclaredMethods()) {

            RedisQuery redisQuery = method.getAnnotation(RedisQuery.class);
            String queryStr = redisQuery.value();

            LuaContext luaContext = null;
            String sha1;

            if (isRawMethod(method)) {
                sha1 = jedis.scriptLoad(queryStr);
            } else {
                luaContext = new LuaContextExtractor().render(queryStr, method);
                sha1 = jedis.scriptLoad(luaContext.getRenderedLuaString());
            }

            Mapper methodMapper = method.getAnnotation(Mapper.class);
            RedisResultMapper mapper = null;
            if (methodMapper != null) {
                mapper = methodMapper.value().newInstance();
            }

            contexts.put(method, new MethodContext(sha1, mapper, luaContext));
        }

        methodContextCache.putIfAbsent(t, contexts);
    }

    /**
     * If the method does not have @Bind or @BindKey it is assumed to be a call without script bindings
     * @param method the function to check on
     * @return true if the method is considered not to have any bindings needed
     */
    private boolean isRawMethod(Method method) {
        return (method.getParameterTypes().length == 0)
                || (method.getParameterTypes()[0] == List.class);
    }
}
