package com.lithium.dbi.rdbi.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.recipes.cache.SerializationHelper;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "integration")
public class RedisCircularBufferTest {
    public String circularBufferKey = "REDISCIRCULARBUFFER" + UUID.randomUUID().toString();
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
    public void getFromEmptyTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 5, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void clearTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 5, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        circularBuffer.add(UUID.randomUUID());
        circularBuffer.add(UUID.randomUUID());
        circularBuffer.add(UUID.randomUUID());

        assertEquals(circularBuffer.size(), 3);

        circularBuffer.clear();

        assertEquals(circularBuffer.size(), 0);
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void bufferIsCircularTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 5, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final UUID third = UUID.randomUUID();
        final UUID fourth = UUID.randomUUID();
        final UUID fifth = UUID.randomUUID();
        final UUID sixth = UUID.randomUUID();

        circularBuffer.addAll(ImmutableList.of(first, second, third, fourth, fifth, sixth));

        assertTrue(circularBuffer.containsAll(ImmutableList.of(second, third, fourth, fifth, sixth)));
        assertFalse(circularBuffer.contains(first));

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void basicOperationTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 5, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final UUID third = UUID.randomUUID();

        circularBuffer.addAll(ImmutableList.of(first));
        circularBuffer.addAll(ImmutableList.of(second,third));

        assertTrue(circularBuffer.containsAll(ImmutableList.of(first, second, third)));

        circularBuffer.remove(second);

        assertFalse(circularBuffer.contains(second));
        assertTrue(circularBuffer.contains(third));
        assertFalse(circularBuffer.containsAll(ImmutableList.of(first, second)));
        assertTrue(circularBuffer.containsAll(ImmutableList.of(first, third)));

//        assertEquals(circularBuffer.size(), 2);
//
//        assertEquals(circularBuffer.popRange(0, 10), ImmutableList.of(new RedisCircularBuffer.ValueWithScore<>(first, 9),
//                                                           new RedisCircularBuffer.ValueWithScore<>(third, 10)));
//
//        assertTrue(circularBuffer.isEmpty());
//
//        circularBuffer.addAll(ImmutableList.of(second), 5);
//        circularBuffer.add(third);
//        circularBuffer.addAll(ImmutableList.of(first), 0);
//
//        assertEquals(circularBuffer.popRange(0, 0),
//                     ImmutableList.of(new RedisCircularBuffer.ValueWithScore<>(first, 0)));
//
//        assertEquals(circularBuffer.popRange(1, 1),
//                     ImmutableList.of(new RedisCircularBuffer.ValueWithScore<>(third, 10)));

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }
}
