package com.lithium.dbi.rdbi;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.CallbackFilter;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.MethodInterceptor;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

class ProxyFactory {

    private static final MethodInterceptor NO_OP = new MethodNoOpInterceptor();
    private static final CallbackFilter FINALIZE_FILTER = new CallbackFilter() {
        public int accept(Method method) {
            if (method.getName().equals("finalize") &&
                    method.getParameterTypes().length == 0 &&
                    method.getReturnType() == Void.TYPE) {
                return 0; //the NO_OP method interceptor
            }
            return 1; //the everything else method interceptor
        }
    };

    @VisibleForTesting
    final ConcurrentMap<Class<?>, Factory> factoryCache;

    @VisibleForTesting
    final ConcurrentMap<Class<?>, Map<Method, MethodContext>> methodContextCache;

    private final Factory jedisInterceptorFactory;

    ProxyFactory() {
        factoryCache = Maps.newConcurrentMap();
        methodContextCache =  Maps.newConcurrentMap();
        jedisInterceptorFactory = JedisWrapperMethodInterceptor.newFactory();
    }

    JedisWrapperDoNotUse attachJedis(final Jedis jedis) {
        return JedisWrapperMethodInterceptor.newInstance(jedisInterceptorFactory, jedis);
    }

    @SuppressWarnings("unchecked")
    <T> T createInstance(final Jedis jedis, final Class<T> t) {

        Factory factory;
        if (factoryCache.containsKey(t)) {
            return (T) factoryCache.get(t).newInstance(new Callback[]{NO_OP,new MethodContextInterceptor(jedis, methodContextCache.get(t))});
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
            e.setCallbacks(new Callback[]{NO_OP,NO_OP}); //this will be overriden anyway, we set 2 so that it valid for FINALIZE_FILTER
            e.setCallbackFilter(FINALIZE_FILTER);

            factory = (Factory) e.create();
            factoryCache.putIfAbsent(t, factory);
            return (T) factory.newInstance(new Callback[]{NO_OP, new MethodContextInterceptor(jedis, methodContextCache.get(t))});
        }
    }

    private <T> void buildMethodContext(Class<T> t, Jedis jedis) throws IllegalAccessException, InstantiationException {

        if (methodContextCache.containsKey(t)) {
            return;
        }

        Map<Method, MethodContext> contexts = Maps.newHashMap();

        for (Method method : t.getDeclaredMethods()) {

            Query query = method.getAnnotation(Query.class);
            String queryStr = query.value();

            LuaContext luaContext = null;
            String sha1;

            if (isRawMethod(method)) {
                sha1 = jedis.scriptLoad(queryStr);
            } else {
                luaContext = new LuaContextExtractor().render(queryStr, method);
                sha1 = jedis.scriptLoad(luaContext.getRenderedLuaString());
            }

            Mapper methodMapper = method.getAnnotation(Mapper.class);
            ResultMapper mapper = null;
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
