package com.lithium.dbi.rdbi.recipes.channel;

import java.util.List;

import com.google.common.collect.Lists;
import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.Mapper;
import com.lithium.dbi.rdbi.Query;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.ResultMapper;

public class ChannelLuaReceiver implements ChannelReceiver {

    private final RDBI rdbi;

    public static interface DAO {
        @Query(
            "local current_count = redis.call(\"GET\", $countKey$)\n" +
            "if not current_count then\n" +
            "    current_count = 0\n" +
            "else\n" +
            "   current_count = tonumber(current_count)\n" +
            "end\n" +
            "if current_count <= tonumber($lastSeenCount$) then\n" +
            "    return {tostring(0)}\n" +
            "end\n" +
            "local results = redis.call(\"LRANGE\", $listKey$, 0, current_count - tonumber($lastSeenCount$) - 1)\n" +
            "results[#results + 1] = tostring(current_count)\n" +
            "return results"
        )
        @Mapper(GetResultMapper.class)
        public GetResult get(
            @BindKey("countKey") String countKey,
            @BindKey("listKey") String listKey,
            @BindArg("lastSeenCount") Long lastSeenCount
        );

        @Query(
            "local current_count = redis.call(\"GET\", $countKey$)\n" +
            "if not current_count then\n" +
            "    current_count = 0\n" +
            "else\n" +
            "   current_count = tonumber(current_count)\n" +
            "end\n" +
            "if current_count <= tonumber($lastSeenCount$) then\n" +
            "    redis.call(\"SET\", $copyDepthToKey$, current_count)\n" +
            "    return {tostring(0)}\n" +
            "end\n" +
            "local results = redis.call(\"LRANGE\", $listKey$, 0, current_count - tonumber($lastSeenCount$) - 1)\n" +
            "results[#results + 1] = tostring(current_count)\n" +
            "redis.call(\"SET\", $copyDepthToKey$, current_count)\n" +
            "return results"
        )
        @Mapper(GetResultMapper.class)
        public GetResult get(
            @BindKey("countKey") String countKey,
            @BindKey("listKey") String listKey,
            @BindArg("lastSeenCount") Long lastSeenCount,
            @BindArg("copyDepthToKey") String copyDepthToKey
        );

        @Query(
            "local current_count = redis.call(\"GET\", $countKey$)\n" +
            "if not current_count then\n" +
            "    current_count = 0\n" +
            "else\n" +
            "    current_count = tonumber(current_count)\n" +
            "end\n" +
            "return current_count"
        )
        public Long getDepth(@BindKey("countKey") String countKey);

        @Query(
            "local current_count = redis.call(\"GET\", $countKey$)\n" +
            "if not current_count then\n" +
            "    current_count = 0\n" +
            "else\n" +
            "    current_count = tonumber(current_count)\n" +
            "end\n" +
            "redis.call(\"SET\", $copyDepthToKey$, current_count)\n" +
            "return current_count"
        )
        public Long getDepth(@BindKey("countKey") String countKey,
                             @BindKey("copyDepthToKey") String copyDepthToKey);
    }

    public static class GetResultMapper implements ResultMapper<GetResult,List<String>> {

        @Override
        public GetResult map(List<String> result) {

            if (result.size() == 0 || result.size() == 1) {
                return null;
            }

            return new GetResult(Lists.reverse(result.subList(0, result.size() - 1)), Long.valueOf(result.get(result.size() - 1)));
        }
    }

    public ChannelLuaReceiver(RDBI rdbi) {
        this.rdbi = rdbi;
    }

    @Override
    public GetResult get(String channel, Long lastSeenId) {
        return get(channel, lastSeenId, null);
    }

    @Override
    public GetResult get(String channel, Long lastSeenId, String copyDepthToKey) {
        Handle handle = rdbi.open();

        try {
            DAO dao = handle.attach(DAO.class);
            if (copyDepthToKey == null) {
                return dao.get(ChannelPublisher.getChannelDepthKey(channel),
                        ChannelPublisher.getChannelQueueKey(channel),
                        lastSeenId);
            } else {
                return dao.get(ChannelPublisher.getChannelDepthKey(channel),
                        ChannelPublisher.getChannelQueueKey(channel),
                        lastSeenId,
                        copyDepthToKey);
            }
        } finally {
            handle.close();
        }
    }

    @Override
    public Long getDepth(String channel) {
        return getDepth(channel, null);
    }

    @Override
    public Long getDepth(String channel, String copyDepthToKey) {
        Handle handle = rdbi.open();

        try {
            DAO dao = handle.attach(DAO.class);
            if (copyDepthToKey == null) {
                return dao.getDepth(ChannelPublisher.getChannelDepthKey(channel));
            } else {
                return dao.getDepth(ChannelPublisher.getChannelDepthKey(channel), copyDepthToKey);
            }
        } finally {
            handle.close();
        }
    }
}
