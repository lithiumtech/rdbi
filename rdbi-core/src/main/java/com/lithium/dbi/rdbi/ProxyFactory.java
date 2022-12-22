package com.lithium.dbi.rdbi;

import io.opentelemetry.api.trace.Tracer;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class ProxyFactory {

    final ConcurrentMap<Class<?>, DynamicType.Loaded<?>> factoryCache;

    final ConcurrentMap<Class<?>, Map<Method, MethodContext>> methodContextCache;

   // private final Factory jedisInterceptorFactory;

    ProxyFactory() {
        factoryCache = new ConcurrentHashMap<>();
        methodContextCache =  new ConcurrentHashMap<>();
        jedisInterceptorFactory = JedisWrapperMethodInterceptor.newFactory();
    }

    JedisWrapperDoNotUse attachJedis(final Jedis jedis, Tracer tracer) {
        return JedisWrapperMethodInterceptor.newInstance(jedisInterceptorFactory, jedis, tracer);
    }

    @SuppressWarnings("unchecked")
    <T> T createInstance(final Jedis jedis, final Class<T> t) {

        DynamicType.Loaded<T> loaded = getLoadedType(jedis, t);
        try {
            return loaded.getLoaded().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> DynamicType.Loaded<T> getLoadedType(Jedis jedis, Class<T> t) {
//        if (!factoryCache.containsKey(t)) {
//            try {
//                DynamicType.Loaded<T> loaded = buildMethodContext(t, jedis)
//                        .make()
//                        .load(t.getClassLoader());
//
//                factoryCache.put(t, loaded);
//            } catch (InstantiationException | IllegalAccessException e) {
//                throw new RuntimeException(e);
//            }
//        }
        return (DynamicType.Loaded<T>) factoryCache.computeIfAbsent(t, (key) -> {
            DynamicType.Loaded<T> loaded = null;
            try {
                loaded = buildMethodContext(t, jedis)
                        .make()
                        .load(t.getClassLoader());
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            return loaded;
        });
    }

    private <T> DynamicType.Builder<T> buildMethodContext(Class<T> t, Jedis jedis) throws IllegalAccessException, InstantiationException {

//        if (methodContextCache.containsKey(t)) {
//            return;
//        }

        DynamicType.Builder<T> builder = new ByteBuddy()
                .subclass(t);

      //  Map<Method, MethodContext> contexts = new HashMap<>();

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
            ResultMapper<?, ?> mapper = null;
            if (methodMapper != null) {
                mapper = methodMapper.value().newInstance();
            }

            builder.method(ElementMatchers.is(method))
                           .intercept(MethodDelegation.to(new BBMethodContextInterceptor( new MethodContext(sha1, mapper, luaContext))));

           // contexts.put(method, new MethodContext(sha1, mapper, luaContext));
        }

       // methodContextCache.putIfAbsent(t, contexts);
        return builder;
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

    // i don't think we need this because we don't have to lookup finalize, we just don't override it
//    private static class FinalizeFilter implements CallbackFilter {
//        @Override
//        public int accept(Method method) {
//            if (method.getName().equals("finalize") &&
//                    method.getParameterTypes().length == 0 &&
//                    method.getReturnType() == Void.TYPE) {
//                return 0; //the NO_OP method interceptor
//            }
//            return 1; //the everything else method interceptor
//        }
//    }
}
