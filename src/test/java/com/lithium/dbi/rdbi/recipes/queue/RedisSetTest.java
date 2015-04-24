package com.lithium.dbi.rdbi.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.recipes.cache.SerializationHelper;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.UUID;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "integration")
public class RedisSetTest {
    public String setKey = "REDISSET" + UUID.randomUUID().toString();
    final RDBI rdbi = new RDBI(new JedisPool("localhost"));

    public static class UUIDSerialHelper implements SerializationHelper<UUID> {

        @Override
        public UUID decode(String string) {
            return UUID.fromString(string);
        }

        @Override
        public String encode(UUID value) {
            return value.toString();
        }
    }

    @Test
    public void clearTest() {
        final RedisSet<UUID> set = new RedisSet<>(new UUIDSerialHelper(),
                                                  "clearTest",
                                                  setKey,
                                                  rdbi,
                                                  10);

        set.add(UUID.randomUUID());
        set.add(UUID.randomUUID());
        set.add(UUID.randomUUID());

        assertEquals(3, set.size());

        set.clear();

        assertEquals(0, set.size());
        assertTrue(set.isEmpty());
    }

    @Test
    public void basicOperationTest() {
        final RedisSet<UUID> set = new RedisSet<>(new UUIDSerialHelper(),
                                                  "clearTest",
                                                  setKey,
                                                  rdbi,
                                                  10);

        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final UUID third = UUID.randomUUID();

        set.addAll(ImmutableList.of(first), 9);
        set.addAll(ImmutableList.of(second,third));

        set.remove(second);

        assertEquals(2, set.size());

        assertEquals(ImmutableList.of(new RedisSet.ValueWithScore<>(first, 9),
                                      new RedisSet.ValueWithScore<>(third, 10)),
                     set.popRange(0, 10));

        assertTrue(set.isEmpty());

        set.addAll(ImmutableList.of(second), 5);
        set.add(third);
        set.addAll(ImmutableList.of(first), 0);

        assertEquals(ImmutableList.of(new RedisSet.ValueWithScore<>(first, 0)),
                     set.popRange(0, 0));

        assertEquals(ImmutableList.of(new RedisSet.ValueWithScore<>(third, 10)),
                     set.popRange(1, 1));
    }
}
