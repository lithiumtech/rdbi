package com.lithium.dbi.rdbi.recipes.channel;

import com.google.common.collect.Lists;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import javax.inject.Inject;
import java.util.List;

public class ChannelWatchReceiver implements ChannelReceiver {

    private final RDBI rdbi;

    @Inject
    public ChannelWatchReceiver(RDBI rdbi) {
        this.rdbi = rdbi;
    }

    private Long getDepth(Jedis jedis, String channel) {

        String depthStrValue = jedis.get(channel + ":depth" );

        if (depthStrValue == null) {
            return null;
        }

        return Long.valueOf(depthStrValue);
    }

    public GetResult get(final String channel, final Long lastSeenId) {

        Handle handle = rdbi.open();

        try {

            Jedis jedis = handle.jedis();
            jedis.watch(channel + ":depth");

            Long depth = getDepth(jedis, channel);

            if (depth == null) {
                jedis.unwatch();
                return new GetResult();
            }

            Long diff = depth - lastSeenId - 1;

            if (diff < 0) {
                //this means if you give me a number in the future i will return nothing...
                jedis.unwatch();
                return new GetResult();
            }

            Transaction transaction = jedis.multi();
            Response<List<String>> responseMessages = transaction.lrange(channel + ":queue", 0, diff);
            List<Object> objResults = transaction.exec();

            if (objResults == null) {
                return new GetResult(null, depth);
            }

            List<String> results = responseMessages.get();

            if (results == null) {
                return new GetResult(null, depth);
            }

            return new GetResult(Lists.reverse(results), depth);
        } finally {
            handle.close();
        }
    }
}
