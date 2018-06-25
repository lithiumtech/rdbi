package com.lithium.dbi.rdbi.recipes.channel;

import com.google.common.collect.Lists;
import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.Mapper;
import com.lithium.dbi.rdbi.Query;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.ResultMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ChannelLuaReceiver implements ChannelReceiver {

    private final RDBI rdbi;

    public interface DAO {
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
        GetResult get(
                @BindKey("countKey") String countKey,
                @BindKey("listKey") String listKey,
                @BindArg("lastSeenCount") Long lastSeenCount
        );

        @Query(
            "local current_count = redis.call(\"GET\", $countKey$)\n" +
            "if not current_count then\n" +
            "    return {tostring(0)}\n" +
            "else\n" +
            "    current_count = tonumber(current_count)\n" +
            "end\n" +
            "if current_count <= tonumber($lastSeenCount$) then\n" +
            "    return {tostring(current_count)}\n" +
            "end\n" +
            "local results = redis.call(\"LRANGE\", $listKey$, 0, current_count - tonumber($lastSeenCount$) - 1)\n" +
            "results[#results + 1] = tostring(current_count)\n" +
            "return results"
        )
        @Mapper(GetWithDepthResultMapper.class)
        GetResult getAndReturnCurrentDepth(
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
        GetResult get(
                @BindKey("countKey") String countKey,
                @BindKey("listKey") String listKey,
                @BindArg("lastSeenCount") Long lastSeenCount,
                @BindArg("copyDepthToKey") String copyDepthToKey
        );

        @Query(
                "local number_of_channels = ARGV[1]\n" +
                        "local bulk_result = {}\n" +
                        "for i = 1, number_of_channels do\n" +
                        "    local current_count = redis.call(\"GET\", tostring(KEYS[i*2]))\n" +
                        "    if not current_count then\n" +
                        "        current_count = 0\n" +
                        "    else\n" +
                        "        current_count = tonumber(current_count)\n" +
                        "    end\n" +
                        "    if current_count <= tonumber(ARGV[i+1]) then\n" +
                        "        bulk_result[i] = {}\n" +
                        "    else\n" +
                        "        local results = redis.call(\"LRANGE\", KEYS[i*2-1], 0, current_count - tonumber(ARGV[i+1]) + 1)\n" +
                        "        results[#results + 1] = tostring(current_count)\n" +
                        "        bulk_result[i] = results\n" +
                        "    end\n" +
                        "end\n" +
                        "return bulk_result;"
        )
        @Mapper(GetBulkResultMapper.class)
        GetBulkResult getMulti(
                @BindKey("allKeys") List<String> inputKeys,
                @BindArg("allArgs") List<String> inputArgs
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
        Long getDepth(@BindKey("countKey") String countKey);

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
        Long getDepth(@BindKey("countKey") String countKey,
                      @BindKey("copyDepthToKey") String copyDepthToKey);
    }

    public static class GetResultMapper implements ResultMapper<GetResult, List<String>> {

        @Override
        public GetResult map(List<String> result) {

            if (result.size() == 0 || result.size() == 1) {
                return null;
            }

            return new GetResult(Lists.reverse(result.subList(0, result.size() - 1)), Long.valueOf(result.get(result.size() - 1)));
        }
    }

    public static class GetWithDepthResultMapper implements ResultMapper<GetResult, List<String>> {

        @Override
        public GetResult map(List<String> result) {

            if (result.size() == 0) {
                throw new IllegalStateException("unexpected 0 length return from redis lua script");
            }

            if (result.size() == 1) {
                return new GetResult(null, Long.valueOf(result.get(0)));
            }

            return new GetResult(Lists.reverse(result.subList(0, result.size() - 1)), Long.valueOf(result.get(result.size() - 1)));
        }
    }

    public static class GetBulkResultMapper implements ResultMapper<GetBulkResult, List<List<String>>> {

        @Override
        public GetBulkResult map(List<List<String>> result) {

            if (result.size() == 0) {
                return null;
            }

            List<List<String>> listsResult = new ArrayList<>();
            List<Long> listsSizes = new ArrayList<>();

            for (List<String> each : result) {
                if (each.size() == 0) {
                    listsResult.add(each);
                    listsSizes.add(0L);
                    continue;
                }
                listsResult.add(Lists.reverse(each.subList(0, each.size() - 1)));
                listsSizes.add(Long.valueOf(each.get(each.size() - 1)));
            }

            return new GetBulkResult(listsResult, listsSizes);
        }
    }

    public ChannelLuaReceiver(RDBI rdbi) {
        this.rdbi = rdbi;
    }

    @Override
    public GetResult get(String channel, Long lastSeenId) {
        return get(channel, lastSeenId, null);
    }

    /**
     * Gets new data from the channel and returns the current channel depth.
     * This is unlike the get method, that returns the input channel depth if no data was found.
     *
     * This is useful because if the channel was reset, the client will see their id > the channel's id and should reset.
     * This happens on redis clearing etc.
     *
     * @param channel the channel's name
     * @param lastSeenId the last seen id by the client
     * @return GetResult that contains the channel's latest depth
     */
    @Override
    public GetResult getAndReturnCurrentCount(String channel, Long lastSeenId) {

        try (Handle handle = rdbi.open()) {
            DAO dao = handle.attach(DAO.class);

            return dao.getAndReturnCurrentDepth(ChannelPublisher.getChannelDepthKey(channel),
                    ChannelPublisher.getChannelQueueKey(channel),
                    lastSeenId);
        }
    }

    @Override
    public GetResult get(String channel, Long lastSeenId, String copyDepthToKey) {

        try (Handle handle = rdbi.open()) {
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
        }
    }

    public GetBulkResult getMulti(List<String> channels, List<Long> lastSeenIds) {
        try (Handle handle = rdbi.open()) {
            DAO dao = handle.attach(DAO.class);
            List<String> depthKeys = channels.stream().map(ChannelPublisher::getChannelDepthKey).collect(Collectors.toList());
            List<String> queueKeys = channels.stream().map(ChannelPublisher::getChannelQueueKey).collect(Collectors.toList());

            List<String> keysList = new ArrayList<>(depthKeys.size() * 2);
            List<String> argsList = new ArrayList<>(depthKeys.size());
            argsList.add(String.valueOf(depthKeys.size()));

            for (int i = 0; i < channels.size(); i++) {
                keysList.add(queueKeys.get(i));
                keysList.add(depthKeys.get(i));
                argsList.add(String.valueOf(lastSeenIds.get(i)));
            }

            return dao.getMulti(keysList, argsList);
        }
    }

    @Override
    public Long getDepth(String channel) {
        return getDepth(channel, null);
    }

    @Override
    public Long getDepth(String channel, String copyDepthToKey) {
        try (Handle handle = rdbi.open()) {
            final DAO dao = handle.attach(DAO.class);
            if (copyDepthToKey == null) {
                return dao.getDepth(ChannelPublisher.getChannelDepthKey(channel));
            } else {
                return dao.getDepth(ChannelPublisher.getChannelDepthKey(channel), copyDepthToKey);
            }
        }
    }
}
