package com.lithium.dbi.rdbi;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.matcher.ElementMatchers;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

class JedisWrapperMethodInterceptor {

    private final Jedis jedis;
    private final Tracer tracer;
    private final Attributes commonAttributes;

    static Factory newFactory() {
        new ByteBuddy()
                .subclass(JedisWrapperDoNotUse.class)
                .method(ElementMatchers.isMethod())
                .intercept()
        Enhancer e = new Enhancer();
        e.setClassLoader(Jedis.class.getClassLoader());
        e.setSuperclass(JedisWrapperDoNotUse.class);
        e.setCallback(new MethodNoOpInterceptor());
        return (Factory) e.create();
    }

    static JedisWrapperDoNotUse newInstance(final Factory factory, final Jedis realJedis, final Tracer tracer) {
        return (JedisWrapperDoNotUse) factory.newInstance(new JedisWrapperMethodInterceptor(realJedis, tracer));
    }

    private JedisWrapperMethodInterceptor(Jedis jedis, Tracer tracer) {
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
        } finally {
            s.end();
        }
    }
}
