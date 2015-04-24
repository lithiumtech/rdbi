package com.lithium.dbi.rdbi.recipes.set;

import com.google.common.collect.ImmutableList;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.recipes.cache.SerializationHelper;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.UUID;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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

        assertEquals(set.size(), 3);

        set.clear();

        assertEquals(set.size(), 0);
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

        assertTrue(set.containsAll(ImmutableList.of(first, second, third)));

        set.remove(second);

        assertFalse(set.contains(second));
        assertTrue(set.contains(third));
        assertFalse(set.containsAll(ImmutableList.of(first, second)));
        assertTrue(set.containsAll(ImmutableList.of(first, third)));

        assertEquals(set.size(), 2);

        assertEquals(set.popRange(0, 10), ImmutableList.of(new RedisSet.ValueWithScore<>(first, 9),
                                                           new RedisSet.ValueWithScore<>(third, 10)));

        assertTrue(set.isEmpty());

        set.addAll(ImmutableList.of(second), 5);
        set.add(third);
        set.addAll(ImmutableList.of(first), 0);

        assertEquals(set.popRange(0, 0),
                     ImmutableList.of(new RedisSet.ValueWithScore<>(first, 0)));

        assertEquals(set.popRange(1, 1),
                     ImmutableList.of(new RedisSet.ValueWithScore<>(third, 10)));
    }
}
