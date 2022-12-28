package com.lithium.dbi.rdbi;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.TypeCache;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.matcher.ElementMatchers;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class JedisWrapperMethodInterceptor {

    private final Jedis jedis;
    private final Tracer tracer;
    private final Attributes commonAttributes;
    private static final TypeCache<Class<?>> cache = new TypeCache<>();

    static Jedis newInstance(final Jedis realJedis, final Tracer tracer) {

        try {
            Object proxy = cache.findOrInsert(Jedis.class.getClassLoader(), Jedis.class, JedisWrapperMethodInterceptor::newLoadedClass)
                                .getDeclaredConstructor()
                                .newInstance();
            final Field field = proxy.getClass().getDeclaredField("handler");
            field.set(proxy, new JedisWrapperMethodInterceptor(realJedis, tracer));
            return (Jedis) proxy;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
                 NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static Class<? extends JedisWrapperDoNotUse> newLoadedClass() {
        return new ByteBuddy()
                .subclass(JedisWrapperDoNotUse.class)
                .defineField("handler", JedisWrapperMethodInterceptor.class, Visibility.PUBLIC)
                .method(ElementMatchers.isMethod())
                .intercept(MethodDelegation.toField("handler"))
                .make()
                .load(Jedis.class.getClassLoader(), ClassLoadingStrategy.UsingLookup.withFallback(MethodHandles::lookup))
                .getLoaded();
    }

    public JedisWrapperMethodInterceptor(Jedis jedis, Tracer tracer) {
        this.jedis = jedis;
        this.tracer = tracer;
        commonAttributes = Attributes.of(
                AttributeKey.stringKey("db.type"), "redis",
                AttributeKey.stringKey("component"), "rdbi"
                                        );
    }

    @RuntimeType
    public Object intercept(
            @AllArguments Object[] args,
            @Origin Method method) {
        Span s = tracer.spanBuilder(method.getName())
                       .setAllAttributes(commonAttributes)
                       .startSpan();
        if (args.length > 0 && args[0] instanceof String) {
            s.setAttribute("redis.key", (String) args[0]);
        }
        try (Scope ignored = s.makeCurrent()) {
            return method.invoke(jedis, args);
        } catch (JedisException e) {
            s.recordException(e);
            throw e;
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } finally {
            s.end();
        }
    }
}
