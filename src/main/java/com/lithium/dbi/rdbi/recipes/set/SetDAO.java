package com.lithium.dbi.rdbi.recipes.set;

import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Query;

import java.util.List;

public interface SetDAO {
    @Query(
            "local elements = redis.call('ZRANGE', $redisKey$, $lowerRank$, $upperRank$, 'WITHSCORES')\n" +
            "if next(elements) == nil then\n" +
            "   return nil\n" +
            "end\n" +
            "redis.call('ZREMRANGEBYRANK', $redisKey$, $lowerRank$, $upperRank$)\n" +
            "return elements"
    )
    List<String> zPopRangeByRank(@BindKey("redisKey") String redisKey,
                                 @BindKey("lowerRank") long lowerRank,
                                 @BindKey("upperRank") long upperRank);
}
