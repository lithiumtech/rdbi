package com.lithium.rdbi;

import com.google.common.collect.Sets;
import org.stringtemplate.v4.ST;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Set;

class LuaContextExtractor {

    public LuaContext render(String query, Method method) {

        ST st = new ST(query, '$', '$');

        Annotation[][] annotationsParams = method.getParameterAnnotations();
        int paramCounter = 0;
        Set<Integer> keys = Sets.newHashSet();
        int keyCounter = 0;
        int argCounter = 0;
        for ( Annotation[] annotations : annotationsParams) {
            String attribute = null;
            boolean isBind = false;

            for (Annotation annotation : annotations) {
                if (annotation instanceof BindArg) {
                    attribute = ((BindArg) annotation).value();
                    isBind = true;
                    argCounter++;
                    break;
                } else if (annotation instanceof BindKey) {
                    attribute = ((BindKey) annotation).value();
                    keyCounter++;
                    break;
                }
            }

            if (attribute == null) {
                throw new IllegalArgumentException(
                        "Each argument must contain a Bind or BindKey annotation. " +
                        " Parameter at " + paramCounter + " does not have Bind or BindKey annotation.");
            }

            String value;
            if (isBind) {
                value = "ARGV[" + argCounter + "]";
            } else {
                value = "KEY[" + keyCounter + "]";
                keys.add(paramCounter);
            }
            st.add(attribute, value);

            paramCounter++;
        }
        return new LuaContext(st.render(), keys);
    }
}
