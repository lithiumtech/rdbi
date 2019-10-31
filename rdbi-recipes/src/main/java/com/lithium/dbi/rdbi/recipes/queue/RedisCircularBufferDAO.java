package com.lithium.dbi.rdbi.recipes.queue;

import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Query;

public interface RedisCircularBufferDAO {

    @Query(
            "local size = redis.call('RPUSH', $key$, $valueToAdd$)\n" +
                    "   if size > $maxSize$ then\n" +
                    "       redis.call('LPOP', $key$)\n" +
                    "   end\n" +
                    "return true\n"
    )
    boolean add(
            @BindKey("key") String key,
            @BindArg("toAdd") String valueToAdd,
            @BindArg("maxSize") Integer maxSize);

}
