package com.lithium.dbi.rdbi;

import io.opentelemetry.api.trace.Tracer;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class ProxyFactory {

    final Map<Class<?>, Class<?>> cache = new ConcurrentHashMap<>();
    final Map<Class<?>, Map<Method, MethodContext>> methodContextCache = new ConcurrentHashMap<>();

    Jedis attachJedis(final Jedis jedis, Tracer tracer) {
        return JedisWrapperMethodInterceptor.newInstance(jedis, tracer);
    }

    @SuppressWarnings("unchecked")
    <T> T createInstance(final Jedis jedis, final Class<T> t) {
        try {
            BBMethodContextInterceptor interceptor = new BBMethodContextInterceptor(jedis, getMethodMethodContextMap(t, jedis));
            Object instance = get(t).getDeclaredConstructor().newInstance();
            final Field field = instance.getClass().getDeclaredField("handler");
            field.set(instance, interceptor);
            return (T) instance;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
                 NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    boolean isCached(Class<?> t) {
        return cache.get(t) != null;
    }


    private Class<?> get(Class<?> t) throws IllegalAccessException, InstantiationException {
        Class<?> cached = cache.get(t);
        if (cached != null) {
            return cached;
        } else {
            Class<?> newClass = buildClass(t);
            cache.putIfAbsent(t, newClass);
            return cache.get(t);
        }
    }

    private <T> Class<? extends T> buildClass(Class<T> t) throws IllegalAccessException, InstantiationException {


        return new ByteBuddy()
                .subclass(t, ConstructorStrategy.Default.DEFAULT_CONSTRUCTOR)
                .defineField("handler", BBMethodContextInterceptor.class, Visibility.PUBLIC)
                .method(ElementMatchers.any())
                .intercept(MethodDelegation.toField("handler"))
                .make()
                .load(getClass().getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)  // this works for everything BUT the DAO tests in rdbi core
                .getLoaded();

    }

    private <T> Map<Method, MethodContext> getMethodMethodContextMap(Class<T> t, Jedis jedis) throws InstantiationException, IllegalAccessException {
        Map<Method, MethodContext> cached = methodContextCache.get(t);
        if (cached != null) {
            return cached;
        }

        Map<Method, MethodContext> contexts = new HashMap<>();
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
                try {
                    mapper = methodMapper.value().getDeclaredConstructor().newInstance();
                } catch (InvocationTargetException | NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            }
            contexts.put(method, new MethodContext(sha1, mapper, luaContext));
        }
        methodContextCache.putIfAbsent(t, Collections.unmodifiableMap(contexts));
        return methodContextCache.get(t);
    }

    /**
     * If the method does not have @Bind or @BindKey it is assumed to be a call without script bindings
     *
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
