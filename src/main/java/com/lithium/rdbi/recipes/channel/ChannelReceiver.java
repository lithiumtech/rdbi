package com.lithium.rdbi.recipes.channel;

public interface ChannelReceiver {
    GetResult get(final String channel, final Long lastSeenId);
}
