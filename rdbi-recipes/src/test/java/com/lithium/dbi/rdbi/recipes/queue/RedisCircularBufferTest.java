package com.lithium.dbi.rdbi.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.recipes.cache.SerializationHelper;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
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
    public void circularAddTest() {
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
        assertEquals(circularBuffer.size(), 5);

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void basicAddTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 3, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final UUID third = UUID.randomUUID();

        circularBuffer.add(first);
        assertEquals(circularBuffer.size(), 1);

        circularBuffer.add(second);
        assertEquals(circularBuffer.size(), 2);
        assertEquals(circularBuffer.peek(), first);

        circularBuffer.add(third);
        assertEquals(circularBuffer.size(), 3);
        assertEquals(circularBuffer.peek(), first);
        assertTrue(circularBuffer.containsAll(ImmutableList.of(first, second, third)));

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void basicOfferTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 3, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final UUID third = UUID.randomUUID();

        circularBuffer.offer(first);
        assertEquals(circularBuffer.size(), 1);

        circularBuffer.offer(second);
        assertEquals(circularBuffer.size(), 2);
        assertEquals(circularBuffer.peek(), first);

        circularBuffer.offer(third);
        assertEquals(circularBuffer.size(), 3);
        assertEquals(circularBuffer.peek(), first);
        assertTrue(circularBuffer.containsAll(ImmutableList.of(first, second, third)));

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }

    public void basicAddAllTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 3, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final UUID third = UUID.randomUUID();

        circularBuffer.addAll(ImmutableList.of(first, second, third));
        assertEquals(circularBuffer.size(), 3);
        assertEquals(circularBuffer.peek(), first);
        assertTrue(circularBuffer.containsAll(ImmutableList.of(first, second, third)));

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void basicRemoveTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 3, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final UUID third = UUID.randomUUID();
        circularBuffer.addAll(ImmutableList.of(first, second, third));
        assertEquals(circularBuffer.size(), 3);
        assertEquals(circularBuffer.peek(), first);
        assertTrue(circularBuffer.containsAll(ImmutableList.of(first, second, third)));

        circularBuffer.remove();
        assertEquals(circularBuffer.size(), 2);
        assertEquals(circularBuffer.peek(), second);
        assertTrue(circularBuffer.containsAll(ImmutableList.of(second, third)));

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void basicPollTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 3, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final UUID third = UUID.randomUUID();
        circularBuffer.addAll(ImmutableList.of(first, second, third));
        assertEquals(circularBuffer.size(), 3);
        assertEquals(circularBuffer.peek(), first);
        assertTrue(circularBuffer.containsAll(ImmutableList.of(first, second, third)));

        circularBuffer.poll();
        assertEquals(circularBuffer.size(), 2);
        assertEquals(circularBuffer.peek(), second);
        assertTrue(circularBuffer.containsAll(ImmutableList.of(second, third)));

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void removeFromEmptyTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 5, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        assertThrows(NoSuchElementException.class, () -> {
            circularBuffer.remove();
        });
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void pollFromEmptyTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 5, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        UUID result = circularBuffer.poll();
        assertEquals(result, null);
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void basicElementTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 3, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        circularBuffer.addAll(ImmutableList.of(first, second));
        assertEquals(circularBuffer.size(), 2);
        assertEquals(circularBuffer.element(), first);
        assertTrue(circularBuffer.containsAll(ImmutableList.of(first, second)));

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void elementFromEmptyTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 5, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        assertThrows(NoSuchElementException.class, () -> {
            circularBuffer.element();
        });
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void peekFromEmptyTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 5, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        UUID result = circularBuffer.peek();
        assertEquals(result, null);
    }

    @Test
    public void basicPeekAllTest() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 5, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final UUID third = UUID.randomUUID();

        circularBuffer.addAll(ImmutableList.of(first, second, third));
        assertEquals(circularBuffer.size(), 3);
        assertEquals(circularBuffer.peek(), first);
        assertTrue(circularBuffer.containsAll(ImmutableList.of(first, second, third)));

        List<UUID> peekedList = circularBuffer.peekAll();
        assertEquals(peekedList, ImmutableList.of(first, second, third));

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void peekAllFromEmpty() {
        final RedisCircularBuffer<UUID> circularBuffer = new RedisCircularBuffer(rdbi, circularBufferKey, 5, new UUIDSerialHelper());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());

        List<UUID> peekedList = circularBuffer.peekAll();
        assertEquals(peekedList, ImmutableList.of());

        circularBuffer.clear();
        assertTrue(circularBuffer.isEmpty());
    }

    @Test
    public void circularBufferTTL() throws InterruptedException {
        final int ttlInSeconds = 5;
        final RedisCircularBuffer<UUID> buffer = new RedisCircularBuffer<>(rdbi, circularBufferKey, 5, new UUIDSerialHelper(), ttlInSeconds);

        buffer.clear();
        assertTrue(buffer.isEmpty());

        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        buffer.addAll(ImmutableList.of(first, second));
        assertEquals(buffer.size(), 2);
        assertEquals(buffer.peek(), first);
        assertTrue(buffer.containsAll(ImmutableList.of(first, second)));

        Thread.sleep(ttlInSeconds * 1000);
        assertTrue(buffer.isEmpty());
    }
}
