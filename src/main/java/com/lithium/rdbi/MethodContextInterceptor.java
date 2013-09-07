package com.lithium.rdbi;

import com.google.common.collect.Lists;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

class MethodContextInterceptor implements MethodInterceptor {

    private final Jedis jedis;
    private final Map<Method, MethodContext> contexts;

    public MethodContextInterceptor(Jedis jedis, Map<Method, MethodContext> contexts) {
        this.jedis = jedis;
        this.contexts = contexts;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {

        MethodContext context = contexts.get(method);

        Object ret = context.hasDynamicLists() ? callEvalDynamicList(context, objects)
                                               : callEval(context, objects);

        if (ret == null) {
            return null;
        }

        if (contexts.get(method).getMapper() != null) {
            return contexts.get(method).getMapper().map(ret);
        } else {
            return ret;
        }
    }

    @SuppressWarnings("unchecked")
    private Object callEval(MethodContext context, Object[] objects) {

        List<String> keys = objects.length > 0 ? (List<String>) objects[0] : null;
        List<String> argv = objects.length > 1 ? (List<String>) objects[1] : null;
        return jedis.evalsha(context.getSha1(), keys, argv);
    }

    private Object callEvalDynamicList(MethodContext context, Object[] objects) {

        List<String> keys = Lists.newArrayList();
        List<String> argv = Lists.newArrayList();

        for (int i = 0; i < objects.length; i++) {
            if (context.getLuaContext().isKey(i)) {
                keys.add(objects[i].toString());
            } else {
                argv.add(objects[i].toString());
            }
        }

        return  jedis.evalsha(context.getSha1(), keys, argv);
    }
}
