package com.lithium.dbi.rdbi.recipes.channel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.lithium.dbi.rdbi.testutil.Utils.assertTiming;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

@Test(groups = "integration")
public class ChannelPublisherTest {

    @Test
    public void testPublishChannelLuaPerformanceTest() throws InterruptedException {

        final Set<String> channel = ImmutableSet.of("channel1", "channel2", "channel3", "channel4", "channel5");

        final ChannelPublisher channelPublisher = new ChannelPublisher(new RDBI(new JedisPool("localhost", 6379)));
        channelPublisher.resetChannels(channel);

        final List<String> value = ImmutableList.of("value1");

        assertTiming(1500, TimeUnit.MILLISECONDS,
                     () -> IntStream.range(0, 1000).forEach(i -> channelPublisher.publish(channel, value)),
                     () -> IntStream.range(0, 1000).forEach(i -> channelPublisher.publish(channel, value))
                    );
    }

    @Test
    public void simpleInsertTest() {

        final RDBI rdbi = new RDBI(new JedisPool("localhost", 6379));
        final ChannelPublisher channelPublisher = new ChannelPublisher(rdbi);
        channelPublisher.resetChannel("channel1");
        channelPublisher.publish("channel1", "Hello");
        channelPublisher.publish("channel1", ImmutableList.of("World"));

        final ChannelReceiver receiver = new ChannelLuaReceiver(rdbi);
        GetResult result = receiver.get("channel1", 0L);

        assertNotNull(result);
        assertEquals(result.getDepth(), (Long) 2L);
        assertEquals("Hello", result.getMessages().get(0));
        assertEquals("World", result.getMessages().get(1));
    }

    @Test
    public void copyDepthOnGetTest() {

        final Set<String> channel = ImmutableSet.of("channel1");

        final RDBI rdbi = new RDBI(new JedisPool("localhost", 6379));
        final ChannelPublisher channelPublisher = new ChannelPublisher(rdbi);
        channelPublisher.resetChannels(channel);

        channelPublisher.publish(channel, ImmutableList.of("Hello", "World"));

        // Test in bounds get case
        final ChannelReceiver receiver = new ChannelLuaReceiver(rdbi);
        GetResult result = receiver.get("channel1", 0L, "channel1:processed");
        assertNotNull(result);

        Handle handle = rdbi.open();
        try {
            String copiedDepth = handle.jedis().get("channel1:processed");
            assertNotNull(copiedDepth);
            assertEquals(2L, Long.parseLong(copiedDepth));

            handle.jedis().del("channel1:processed");
        } finally {
            handle.close();
        }

        // Test out of bounds get case
        result = receiver.get("channel1", 3L, "channel1:processed");
        assertNull(result);

        handle = rdbi.open();
        try {
            String copiedDepth = handle.jedis().get("channel1:processed");
            assertNotNull(copiedDepth);
            assertEquals(2L, Long.parseLong(copiedDepth));

            handle.jedis().del("channel1:processed");
        } finally {
            handle.close();
        }
    }

    @Test
    public void getDepthTest() {
        final Set<String> channel = ImmutableSet.of("channel1");

        final RDBI rdbi = new RDBI(new JedisPool("localhost", 6379));
        final ChannelPublisher channelPublisher = new ChannelPublisher(rdbi);
        channelPublisher.resetChannels(channel);

        channelPublisher.publish(channel, ImmutableList.of("Hello", "World"));

        final ChannelReceiver receiver = new ChannelLuaReceiver(rdbi);
        Long result = receiver.getDepth("channel1");
        assertEquals(2L, (long) result);

        result = receiver.getDepth("channel1", "channel1:processed");
        assertEquals(2L, (long) result);

        try (Handle handle = rdbi.open()) {
            String copiedDepth = handle.jedis().get("channel1:processed");
            assertNotNull(copiedDepth);
            assertEquals(2L, (long) Long.parseLong(copiedDepth));

            handle.jedis().del("channel1:processed");
        }
    }

    @Test
    public void testPublishChannelPerformanceTest() {

        final Set<String> channel = ImmutableSet.of("channel1", "channel2", "channel3", "channel4", "channel5");

        final RDBI rdbi = new RDBI(new JedisPool("localhost", 6379));
        final ChannelPublisher channelPublisher = new ChannelPublisher(rdbi);
        channelPublisher.resetChannels(channel);

        final List<String> value = ImmutableList.of("value1");

        assertTiming(1500, TimeUnit.MILLISECONDS,
                     () -> IntStream.range(0, 1000).forEach(i -> channelPublisher.publish(channel, value)),
                     () -> IntStream.range(0, 1000).forEach(i -> channelPublisher.publish(channel, value))
                    );

        final ChannelReceiver receiver = new ChannelLuaReceiver(rdbi);

        assertTiming(1500, TimeUnit.MILLISECONDS,
                     () -> IntStream.range(0, 1000).forEach(i -> channelPublisher.publish(channel, value)),
                     () -> IntStream.range(0, 1000).forEach(i -> channelPublisher.publish(channel, value))
                    );

    }
}
