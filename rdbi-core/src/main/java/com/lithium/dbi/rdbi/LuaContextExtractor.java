package com.lithium.dbi.rdbi;

import org.antlr.stringtemplate.StringTemplate;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

class LuaContextExtractor {

    public LuaContext render(String query, Method method) {

        StringTemplate st = new StringTemplate(query);

        Annotation[][] annotationsParams = method.getParameterAnnotations();
        int paramCounter = 0;
        Set<Integer> keys = new HashSet<>();
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
                value = "KEYS[" + keyCounter + "]";
                keys.add(paramCounter);
            }
            st.setAttribute(attribute, value);

            paramCounter++;
        }
        return new LuaContext(st.toString(), keys);
    }
}
