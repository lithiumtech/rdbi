package com.lithium.dbi.rdbi.recipes.channel;

import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.Mapper;
import com.lithium.dbi.rdbi.Query;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.ResultMapper;

import java.util.List;

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
    }

    public static class GetResultMapper implements ResultMapper<GetResult,List<String>> {

        @Override
        public GetResult map(List<String> result) {

            if (result.size() == 0 || result.size() == 1) {
                return null;
            }

            return new GetResult(result.subList(0, result.size() - 2), Long.valueOf(result.get(result.size() - 1)));
        }
    }

    public ChannelLuaReceiver(RDBI rdbi) {
        this.rdbi = rdbi;
    }

    @Override
    public GetResult get(String channel, Long lastSeenId) {

        Handle handle = rdbi.open();

        try {
            return handle.attach(DAO.class).get(ChannelPublisher.getChannelDepthKey(channel),
                                         ChannelPublisher.getChannelQueueKey(channel),
                                         lastSeenId);
        } finally {
            handle.close();
        }
    }
}
