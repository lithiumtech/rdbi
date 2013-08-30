package com.lithium.rdbi;

import com.beust.jcommander.internal.Sets;
import org.stringtemplate.v4.ST;
import redis.clients.jedis.Jedis;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Set;

public class ANTLRTest {

    public static class RenderedResult {


        private final String renderedLuaString;
        private final Set<Integer> keys;

        public RenderedResult(String renderedLuaString, Set<Integer> keys) {
            this.renderedLuaString = renderedLuaString;
            this.keys = keys;
        }

        public String getRenderedLuaString() {
            return renderedLuaString;
        }

        public Set<Integer> getKeys() {
            return keys;
        }

        @Override
        public String toString() {
            return "RenderedResult{" +
                    "renderedLuaString='" + renderedLuaString + '\'' +
                    ", keys=" + keys +
                    '}';
        }
    }

    public static class LuaRenderer {
        RenderedResult render(String query, Method method) {

            ST st = new ST(query, '$', '$');

            Annotation[][] annotationsParams = method.getParameterAnnotations();
            int paramCounter = 0;
            Set<Integer> keys = Sets.newHashSet();
            int keyCounter = 0;
            int argvCounter = 0;
            for ( Annotation[] annotations : annotationsParams) {
                String attribute = null;
                boolean isBind = false;

                for (Annotation annotation : annotations) {
                    if (annotation instanceof Bind) {
                        attribute = ((Bind) annotation).value();
                        isBind = true;
                        argvCounter++;
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
                    value = "ARGV[" + argvCounter + "]";
                } else {
                    value = "KEY[" + keyCounter + "]";
                    keys.add(paramCounter);
                }
                st.add(attribute, value);

                paramCounter++;
            }

            return new RenderedResult(st.render(), keys);
        }
    }

    public static class Tes {
        public void abba(@Bind("a") String a, @Bind("b") String b, @BindKey("c") String c) {}
    }

    public static void main(String[] args) throws NoSuchMethodException {
        System.out.println(new LuaRenderer().render("redis.call('SET',  $a$, $b$, $c$); return 0;", Tes.class.getDeclaredMethod("abba", String.class, String.class, String.class)));
    }
}
