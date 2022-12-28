package com.lithium.dbi.rdbi;

import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BBMethodContextInterceptor {

    private final Jedis jedis;
    private final Map<Method, MethodContext> contexts;


    public BBMethodContextInterceptor(Jedis jedis, Map<Method, MethodContext> contexts) {
        this.jedis = jedis;
        this.contexts = contexts;
    }

    @RuntimeType
    public Object intercept(@AllArguments Object[] args,
                            @Origin Method method) {

        MethodContext context = contexts.get(method);

        Object ret = context.hasDynamicLists() ? callEvalDynamicList(context, args)
                                               : callEval(context, args);
        if (ret == null) {
            return null;
        }


        if (context.getMapper() != null) {
            // here we need to adjust for the expected input to the mapper
            Class<?> parameterType;
            try {
                parameterType = context.getMapper().getClass().getMethod("map", Integer.class).getParameterTypes()[0];
            } catch (NoSuchMethodException e) {
                parameterType = null;
            }
            return context.getMapper().map(adjust(parameterType, ret));
        } else {
            return adjust(method.getReturnType(), ret);
        }
    }

    private Object adjust(Class<?> c, Object ret) {
        // problem here is we are getting a LONG but expected an int
        // did this work on older jdks? doesn't seem to be a cglib vs bytebuddy thing
        // java.lang.ClassCastException: class java.lang.Long cannot be cast to class java.lang.Integer (java.lang.Long and java.lang.Integer are in module java.base of loader 'bootstrap')
        // i am not sure why this wasn't required before, and there's probably a utility out there
        // to do it better the primary thing to handle is explicit cast to int where jedis returns a number
        // as a long
        if (c == null) {
            return ret;
        } else if ((c.equals(Integer.TYPE) || c.isAssignableFrom(Integer.class)) && ret instanceof Long) {
            return ((Long) ret).intValue();
        } else {
            return ret;
        }
    }


    @SuppressWarnings("unchecked")
    private Object callEval(MethodContext context, Object[] objects) {

        List<String> keys = objects.length > 0 ? (List<String>) objects[0] : new ArrayList<>();
        List<String> argv = objects.length > 1 ? (List<String>) objects[1] : new ArrayList<>();

        return evalShaHandleReloadScript(context, keys, argv);
    }

    private Object callEvalDynamicList(MethodContext context, Object[] objects) {

        List<String> keys = new ArrayList<>();
        List<String> argv = new ArrayList<>();

        for (int i = 0; i < objects.length; i++) {
            if (context.getLuaContext().isKey(i)) {
                keys.add(objects[i].toString());
            } else {
                argv.add(objects[i].toString());
            }
        }

        return evalShaHandleReloadScript(context, keys, argv);
    }

    private Object evalShaHandleReloadScript(MethodContext context, List<String> keys, List<String> argv) {
        try {
            return jedis.evalsha(context.getSha1(), keys, argv);
        } catch (JedisDataException e) {
            if (e.getMessage() != null && e.getMessage().contains("NOSCRIPT")) {
                //If it throws again, we can back-off or we can just let it throw again. In this case, I think we should
                //let it throw because most likely will be trying the same thing again and hopefully it will succeed later.
                final String newSha = jedis.scriptLoad(context.getLuaContext().getRenderedLuaString());
                if (!newSha.equals(context.getSha1())) {
                    throw new IllegalStateException("sha should match but they did not");
                }
                return jedis.evalsha(context.getSha1(), keys, argv);
            } else {
                throw e;
            }
        }
    }
}
