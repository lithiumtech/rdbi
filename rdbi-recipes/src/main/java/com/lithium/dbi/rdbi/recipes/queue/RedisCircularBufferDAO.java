package com.lithium.dbi.rdbi.recipes.queue;

import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Query;

public interface RedisCircularBufferDAO {

   /* For reference: https://redis.io/commands/expire, https://redis.io/commands/persist
    * Creates a circular buffer with added values and a TTL in seconds associated with the key.
    * That is, after timeToLiveInSeconds has elapsed and the buffer has not had anything added to it,
    * all values associated with that key will be deleted. It should be noted that if at any point
    * another value is added to the buffer, the ttl will be refreshed to whatever's passed in
    *
    * Using a TTL of 0 will result in buffer persisting until the inevitable heat-death of the universe.
    * It should also be noted that calling this method with a TTL of 0 on a previously
    * volatile key WILL CHANGE THE KEY BACK TO A PERSISTENT ONE
    * */
    @Query(
            "local size = redis.call('RPUSH', $key$, $valueToAdd$)\n" +
                    "if size > tonumber($maxSize$) then\n" +
                    "   local start = size - tonumber($maxSize$)\n" +
                    "   local stop = start + tonumber($maxSize$)\n" +
                    "   redis.call('LTRIM', $key$, start, stop)\n" +
                    "   size = size - 1\n" +
                    "end\n" +
                    "if tonumber($timeToLiveInSeconds$) > 0 then\n" +
                    "   redis.call('EXPIRE', $key$, tonumber($timeToLiveInSeconds$))\n" +
                    "else\n" +
                    "   redis.call('PERSIST', $key$)\n" +
                    "end\n" +
                    "return size\n"
    )
    int add(
            @BindKey("key") String key,
            @BindArg("valueToAdd") String valueToAdd,
            @BindArg("maxSize") Integer maxSize,
            @BindArg("timeToLiveInSeconds") Integer timeToLiveInSeconds);

}
