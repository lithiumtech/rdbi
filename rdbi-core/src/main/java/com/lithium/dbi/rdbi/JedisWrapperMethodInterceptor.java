package com.lithium.dbi.rdbi;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.lang.reflect.Method;
import java.util.Arrays;

class JedisWrapperMethodInterceptor implements MethodInterceptor {

    private final Jedis jedis;
    private final Tracer tracer;
    private boolean jedisBusted;
    private final Attributes commonAttributes;

    static Factory newFactory() {
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
        jedisBusted = false;
    }

    @Override
    public Object intercept(Object o, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
        if ( method.getName().equals("__rdbi_isJedisBusted__")) {
            return jedisBusted;
        }
        Span s = tracer.spanBuilder(method.getName())
                .setAttribute("db.type", "redis")
                .setAllAttributes(commonAttributes)
                .startSpan();
        if (args.length > 0 && args[0] instanceof String) {
            s.setAttribute("redis.key", (String) args[0]);
        }
        try (Scope scope = s.makeCurrent()) {
            return methodProxy.invoke(jedis, args);
        } catch (JedisException e) {
            jedisBusted = true;
            s.recordException(e);
            throw e;
        } finally {
            s.end();
        }
    }
}
