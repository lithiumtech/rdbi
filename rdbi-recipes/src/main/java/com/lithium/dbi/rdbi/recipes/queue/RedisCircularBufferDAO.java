package com.lithium.dbi.rdbi.recipes.queue;

import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Query;

public interface RedisCircularBufferDAO {

    @Query(
            "local size = redis.call('RPUSH', $key$, $valueToAdd$)\n" +
                    "if size > tonumber($maxSize$) then\n" +
                    "   local start = size - tonumber($maxSize$)\n" +
                    "   local stop = start + tonumber($maxSize$)\n" +
                    "   redis.call('LTRIM', $key$, start, stop)\n" +
                    "   size = size - 1\n" +
                    "end\n" +
                    "return size\n"
    )
    int add(
            @BindKey("key") String key,
            @BindArg("valueToAdd") String valueToAdd,
            @BindArg("maxSize") Integer maxSize);

}
