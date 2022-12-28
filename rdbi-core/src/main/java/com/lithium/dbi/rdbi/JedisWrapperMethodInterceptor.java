package com.lithium.dbi.rdbi;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.TypeCache;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.matcher.ElementMatchers;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.Callable;

public class JedisWrapperMethodInterceptor {

    private final Jedis jedis;
    private final Tracer tracer;
    private final Attributes commonAttributes;
    private static final TypeCache<Key> cache = new TypeCache<>();


    static class Key {
        private final Class<?> klass;
        private final long hashCode;

        Key(Object toCache) {
            this(toCache.getClass(), System.identityHashCode(toCache));
        }

        Key(Class<?> klass, long hashCode){
            this.klass = klass;
            this.hashCode = hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;

            if (hashCode != key.hashCode) return false;
            return Objects.equals(klass, key.klass);
        }

        @Override
        public int hashCode() {
            return Objects.hash(klass, hashCode);
        }
    }

    static Jedis newInstance(final Jedis realJedis, final Tracer tracer) {

        try {
            return (Jedis) cache.findOrInsert(Jedis.class.getClassLoader(), new Key(realJedis), () ->
                                        newLoadedClass(realJedis, tracer))
                                .getDeclaredConstructor()
                                .newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Class<? extends JedisWrapperDoNotUse> newLoadedClass(Jedis realJedis, Tracer tracer) {
        return new ByteBuddy()
                .subclass(JedisWrapperDoNotUse.class)
                .method(ElementMatchers.isMethod())
                .intercept(MethodDelegation.to(new JedisWrapperMethodInterceptor(realJedis, tracer), "intercept"))
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
            @Origin Method method,
            @SuperCall Callable<?> callable) {
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
