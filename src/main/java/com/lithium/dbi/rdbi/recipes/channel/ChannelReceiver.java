package com.lithium.dbi.rdbi.recipes.channel;

public interface ChannelReceiver {
    GetResult get(final String channel, final Long lastSeenId);
}
