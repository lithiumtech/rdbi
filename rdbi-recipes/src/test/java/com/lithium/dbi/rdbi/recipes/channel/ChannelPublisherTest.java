package com.lithium.dbi.rdbi.recipes.channel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
        final AtomicBoolean thread1Finished = new AtomicBoolean(false);
        final AtomicBoolean thread2Finished = new AtomicBoolean(false);


        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                channelPublisher.publish(channel, value);

                if (Thread.interrupted()) {
                    return;
                }
            }
            thread1Finished.set(true);
        });
        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                channelPublisher.publish(channel, value);
            }
            thread2Finished.set(true);
        });

        thread1.start();
        thread2.start();

        long timeToFinish = 1500;
        thread1.join(timeToFinish);
        thread2.join(timeToFinish);

        if (!thread1Finished.get() && !thread2Finished.get()) {
            fail("Did not finish in time");
        }
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

    static void timedThreads(long timeLimit, TimeUnit unit, Runnable... runnables) {
        long timeOut = timeLimit * 10;
        long start = System.currentTimeMillis();


        List<CompletableFuture<Void>> futures = Arrays.stream(runnables)
                                                      .map(CompletableFuture::runAsync)
                                                      .collect(Collectors.toList());

        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));

        try {
            all.get(timeOut, unit);
        } catch (TimeoutException e) {
            fail(String.format("did not complete within time limit %d %s, or timeout %d %s", timeLimit, unit, timeOut, unit));
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        long elapsed = System.currentTimeMillis() - start;
        if (elapsed > timeLimit) {
            fail(String.format("Did not finish in time. Expected to finish in %d %s, but finished in %d", timeLimit, unit, elapsed));
        }
    }

    @Test
    public void testPublishChannelPerformanceTest() throws InterruptedException {

        final Set<String> channel = ImmutableSet.of("channel1", "channel2", "channel3", "channel4", "channel5");

        final RDBI rdbi = new RDBI(new JedisPool("localhost", 6379));
        final ChannelPublisher channelPublisher = new ChannelPublisher(rdbi);
        channelPublisher.resetChannels(channel);

        final List<String> value = ImmutableList.of("value1");

        timedThreads(1500, TimeUnit.MILLISECONDS,
                     () -> {
                         for (int i = 0; i < 1000; i++) {
                             channelPublisher.publish(channel, value);
                         }
                     },
                     () -> {
                         for (int i = 0; i < 1000; i++) {
                             channelPublisher.publish(channel, value);
                         }
                     }
                    );

        final ChannelReceiver receiver = new ChannelLuaReceiver(rdbi);

        timedThreads(1500, TimeUnit.MILLISECONDS,
                     () -> {
                         for (int i = 0; i < 100; i++) {
                             receiver.get(channel.iterator().next(), 900L);
                         }
                     },
                     () -> {
                         for (int i = 0; i < 100; i++) {
                             receiver.get(channel.iterator().next(), 900L);
                         }
                     }
                    );

    }
}
