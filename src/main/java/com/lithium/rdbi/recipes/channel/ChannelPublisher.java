package com.lithium.rdbi.recipes.channel;

import com.lithium.rdbi.JedisHandle;
import com.lithium.rdbi.RDBI;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;


public class ChannelPublisher {

    private final RDBI rdbi;

    private int CHANNEL_DEPTH = 99; //100 elements
    private int CHANNEL_EXPIRE_IN_SECONDS = 60 *60;

    @Inject
    public ChannelPublisher(RDBI rdbi) {
        this.rdbi = rdbi;
    }

    public void setChannelDepth(int depth) {
        CHANNEL_DEPTH = depth - 1;
    }

    public void setChannelExpireInSeconds(int expireInSeconds) {
        CHANNEL_EXPIRE_IN_SECONDS = expireInSeconds;
    }

    public void resetChannels(final Set<String> channels) {

        JedisHandle handle = rdbi.open();

        try {
            Pipeline pipeline = handle.jedis().pipelined();

            for (String channel : channels) {
                pipeline.del(channel + ":queue");
                pipeline.del(channel + ":depth");
            }
            pipeline.sync();
        } finally {
            handle.close();
        }
    }

    public void publish(final Set<String> channels, final List<String> messages) {

        JedisHandle handle = rdbi.open();

        try {

            if (messages.isEmpty()) {
                return;
            }

            if (channels.isEmpty()) {
                return;
            }

            Transaction transaction = handle.jedis().multi();
            for ( String channel : channels) {
                for (String message : messages) {
                    transaction.lpush(channel + ":queue", message);
                }
            }

            for (String channel : channels) {
                transaction.incrBy(channel + ":depth", messages.size());
                transaction.expire(channel + ":depth", CHANNEL_EXPIRE_IN_SECONDS);
                transaction.ltrim(channel + ":queue", 0, CHANNEL_DEPTH);
                transaction.expire(channel + ":queue", CHANNEL_EXPIRE_IN_SECONDS);
            }

            transaction.exec();
        } finally {
            handle.close();
        }
    }
}
