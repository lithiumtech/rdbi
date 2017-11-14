package com.lithium.dbi.rdbi.recipes.channel;

import java.util.List;

public interface ChannelReceiver {
    GetResult get(String channel, Long lastSeenId);
    GetResult get(String channel, Long lastSeenId, String copyDepthToKey);
    GetBulkResult getMulti(List<String> channels, List<Long> lastSeenIds);
    Long getDepth(String channel);
    Long getDepth(String channel, String copyDepthToKey);
}
