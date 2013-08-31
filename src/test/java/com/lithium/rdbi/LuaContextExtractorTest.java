package com.lithium.rdbi;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class LuaContextExtractorTest {

    public static abstract class MyTestDao {
        public abstract void abba(@Bind("a") String a, @Bind("b") String b, @BindKey("c") String c);
    }

    public static interface MissingOneBindDao {
        public void abba(@Bind("a") String a, String b);
    }

    @Test
    public void testBasicRender() throws NoSuchMethodException {
        assertEquals(
                "redis.call('SET',  ARGV[1], ARGV[2], KEY[1]); return 0;",
                new LuaContextExtractor()
                        .render("redis.call('SET',  $a$, $b$, $c$); return 0;",
                                MyTestDao.class.getDeclaredMethod("abba", String.class, String.class, String.class)).getRenderedLuaString());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testExeceptionThrownForMissingBind() throws NoSuchMethodException {
        new LuaContextExtractor().render("doesn't matter", MissingOneBindDao.class.getDeclaredMethod("abba", String.class, String.class));
    }

}
