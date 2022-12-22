package com.lithium.dbi.rdbi;

import java.util.Arrays;

public class BBMethodContextInterceptor {
    private final MethodContext methodContext;

    public BBMethodContextInterceptor(MethodContext methodContext) {

        this.methodContext = methodContext;
    }

    public Object intercept(Object[] args) {
        // pull in code from methodContextInterceptor
        return Arrays.toString(args);

    }
}
