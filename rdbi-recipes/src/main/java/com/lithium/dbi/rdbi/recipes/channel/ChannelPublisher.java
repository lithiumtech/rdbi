package com.lithium.dbi.rdbi.recipes.channel;

import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import java.util.Collections;
import java.util.List;
import java.util.Set;


public class ChannelPublisher {

    private static final int DEFAULT_CHANNEL_DEPTH = 100;
    private static final int DEFAULT_CHANNEL_EXPIRE_SECONDS = 30 * 24 * 60 * 60; // 30 days

    private final RDBI rdbi;

    private int channelDepth;
    private int channelExpireInSeconds;

    public ChannelPublisher(RDBI rdbi) {
        this.rdbi = rdbi;
        this.channelDepth = DEFAULT_CHANNEL_DEPTH;
        this.channelExpireInSeconds = DEFAULT_CHANNEL_EXPIRE_SECONDS;
    }

    /**
     * Sets the total number of elements to keep in the list.
     * @param depth the maximum elements to retain.
     */
    public void setChannelDepth(int depth) {
        channelDepth = depth;
    }

    /**
     * LTRIM sets the list to exactly the elements specified. We want to keep
     * channelDepth elements, so we keep 0 through (channelDepth - 1).
     *
     * @return index last element to keep.
     */
    private int getTrimDepth() {
        return channelDepth - 1;
    }

    /**
     * Each time an element is published to the channel, we reset the TTL to
     * the value set here.
     * @param expireInSeconds number of seconds to keep channel data after last
     *                        publish event.
     */
    public void setChannelExpireInSeconds(int expireInSeconds) {
        channelExpireInSeconds = expireInSeconds;
    }

    /**
     * Sugar method for deleting all data in a single channel.
     * @param channel to be deleted.
     */
    public void resetChannel(final String channel) {
        resetChannels(Collections.singleton(channel));
    }

    /**
     * Deletes all data in the specified channels.
     * @param channels to be deleted.
     */
    public void resetChannels(final Set<String> channels) {
        try (Handle handle = rdbi.open()) {
            Pipeline pipeline = handle.jedis().pipelined();
            for (String channel : channels) {
                pipeline.del(getChannelQueueKey(channel));
                pipeline.del(getChannelDepthKey(channel));
            }
            pipeline.sync();
        }
    }

    /**
     * Sugar method for publishing a single message to a single channel.
     * @param channel to publish to.
     * @param message sent to channel.
     */
    public void publish(final String channel, final String message) {
        publish(Collections.singleton(channel), Collections.singletonList(message));
    }

    /**
     * Sugar method for publishing multiple messages to a single channel.
     * @param channel to publish to.
     * @param messages sent to channel.
     */
    public void publish(final String channel, final List<String> messages) {
        publish(Collections.singleton(channel), messages);
    }

    /**
     * Publishes each message to each channel.
     *
     * @param channels the channels to send each message.
     * @param messages the messages to send to each channel.
     */
    public void publish(final Set<String> channels, final List<String> messages) {
        if (messages.isEmpty() || channels.isEmpty()) {
            return;
        }
        try (Handle handle = rdbi.open()) {
            final Transaction transaction = handle.jedis().multi();
            for (String channel : channels) {
                final String channelQueueKey = getChannelQueueKey(channel);
                for (String message : messages) {
                    transaction.lpush(channelQueueKey, message);
                }
            }
            for (String channel : channels) {
                final String channelDepthKey = getChannelDepthKey(channel);
                final String channelQueueKey = getChannelQueueKey(channel);
                transaction.incrBy(channelDepthKey, messages.size());
                transaction.ltrim(channelQueueKey, 0, getTrimDepth());
                if (channelExpireInSeconds > 0) {
                    transaction.expire(channelDepthKey, channelExpireInSeconds);
                    transaction.expire(channelQueueKey, channelExpireInSeconds);
                }
            }
            transaction.exec();
        }
    }

    static String getChannelDepthKey(String channel) {
        return channel + ":depth";
    }

    static String getChannelQueueKey(String channel) {
        return channel + ":queue";
    }
}
