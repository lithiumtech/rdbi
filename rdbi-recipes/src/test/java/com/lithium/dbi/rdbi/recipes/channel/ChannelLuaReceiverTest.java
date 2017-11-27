package com.lithium.dbi.rdbi.recipes.channel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.lithium.dbi.rdbi.RDBI;
import org.junit.Ignore;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.Collectors.toList;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

public class ChannelLuaReceiverTest {

    @Test
    public void testChannelReceiveOrder() throws Exception {
        final List<String> channels = ImmutableList.of("channel1");
        final Set<String> channelSet = ImmutableSet.of("channel1");
        final List<Long> lastSeenIds = ImmutableList.of(0L);

        final int messageAmount = 100;
        final List<String> channelValues = new ArrayList<>();

        RDBI rdbi = new RDBI(new JedisPool("localhost"));

        for (int i = 0; i < messageAmount; i++) {
            channelValues.add(UUID.randomUUID().toString());
        }

        final ChannelPublisher channelPublisher = new ChannelPublisher(rdbi);
        channelPublisher.resetChannels(channelSet);

        channelPublisher.publish(channelSet, channelValues);

        final ChannelReceiver channelReceiver = new ChannelLuaReceiver(rdbi);
        GetBulkResult result = channelReceiver.getMulti(channels, lastSeenIds);

        assertEquals(result.getMessages().get(0).size(), messageAmount);

        for (int j = 0; j < result.getMessages().get(0).size(); j++) {
            assertTrue(result.getMessages().get(0).get(j).equals(channelValues.get(j)));
        }

        channelPublisher.resetChannels(channelSet);
    }

    @Test
    public void testDualChannelPublishAndReceive() throws Exception {
        final List<String> channels = ImmutableList.of("channel1", "channel2");
        final Set<String> channelSet = ImmutableSet.of("channel1", "channel2");
        final List<Long> lastSeenIds = ImmutableList.of(0L, 0L);

        final int messageAmount = 100;
        final List<List<String>> channelValues = new ArrayList<>();
        for (String channel : channels) {
            channelValues.add(new ArrayList<>());
        }

        for (List<String> eachList : channelValues) {
            for (int i = 0; i < messageAmount; i++) {
                eachList.add(UUID.randomUUID().toString());
            }
        }

        RDBI rdbi = new RDBI(new JedisPool("localhost"));

        final ChannelPublisher channelPublisher = new ChannelPublisher(rdbi);
        channelPublisher.resetChannels(channelSet);

        for (int i = 0; i < channels.size(); i++) {
            Set<String> channelName = ImmutableSet.of(channels.get(i));
            channelPublisher.publish(channelName, channelValues.get(i));
        }

        final ChannelReceiver channelReceiver = new ChannelLuaReceiver(rdbi);
        GetBulkResult result = channelReceiver.getMulti(channels, lastSeenIds);

        for (int i = 0; i < result.getMessages().size(); i++) {
            for (int j = 0; j < result.getMessages().get(i).size(); j++) {
                assertTrue(result.getMessages().get(i).get(j).equals(channelValues.get(i).get(j)));
            }
        }

        channelPublisher.resetChannels(channelSet);
    }

    @Test
    public void testThreeChannelPublishAndReceive() throws Exception {
        final List<String> channels = ImmutableList.of("channel1", "channel2", "channel3");
        final Set<String> channelSet = ImmutableSet.of("channel1", "channel2", "channel3");
        final List<Long> lastSeenIds = ImmutableList.of(0L, 0L, 0L);

        final int messageAmount = 100;
        final List<List<String>> channelValues = new ArrayList<>();
        for (String channel : channels) {
            channelValues.add(new ArrayList<>());
        }

        for (List<String> eachList : channelValues) {
            for (int i = 0; i < messageAmount; i++) {
                eachList.add(UUID.randomUUID().toString());
            }
        }

        RDBI rdbi = new RDBI(new JedisPool("localhost"));

        final ChannelPublisher channelPublisher = new ChannelPublisher(rdbi);
        channelPublisher.resetChannels(channelSet);

        for (int i = 0; i < channels.size(); i++) {
            Set<String> channelName = ImmutableSet.of(channels.get(i));
            channelPublisher.publish(channelName, channelValues.get(i));
        }

        final ChannelReceiver channelReceiver = new ChannelLuaReceiver(rdbi);
        GetBulkResult result = channelReceiver.getMulti(channels, lastSeenIds);

        for (int i = 0; i < result.getMessages().size(); i++) {
            for (int j = 0; j < result.getMessages().get(i).size(); j++) {
                assertTrue(result.getMessages().get(i).get(j).equals(channelValues.get(i).get(j)));
            }
        }

        channelPublisher.resetChannels(channelSet);
    }

    @Test
    public void testEmptyChannelPublishAndReceive() throws Exception {


        final List<String> channels = ImmutableList.of("channel1");
        final Set<String> channelSet = ImmutableSet.of("channel1");
        final List<Long> lastSeenIds = ImmutableList.of(0L);

        RDBI rdbi = new RDBI(new JedisPool("localhost"));


        final ChannelPublisher channelPublisher = new ChannelPublisher(rdbi);
        channelPublisher.resetChannels(channelSet);

        final int messageAmount = 0;

        final ChannelReceiver channelReceiver = new ChannelLuaReceiver(rdbi);
        GetBulkResult result = channelReceiver.getMulti(channels, lastSeenIds);

        assertEquals(result.getMessages().get(0).size(), messageAmount);

        channelPublisher.resetChannels(channelSet);

    }

    @Test
    public void testMultiThreadedMultiChannelPublishAndReceive() throws InterruptedException {
        final Set<String> channelSet = ImmutableSet.of("channel1", "channel2", "channel3", "channel4", "channel5");
        final int messageAmount = 50;

        RDBI rdbi = new RDBI(new JedisPool("localhost"));

        final ChannelPublisher channelPublisher = new ChannelPublisher(rdbi);
        channelPublisher.resetChannels(channelSet);

        final AtomicBoolean thread1Finished = new AtomicBoolean(false);
        final AtomicBoolean thread2Finished = new AtomicBoolean(false);

        Map<String, Integer> uuidMap = new HashMap<>();

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < messageAmount; i++) {
                    String stringVal = "value" + UUID.randomUUID();
                    uuidMap.put(stringVal, 0);
                    final List<String> value = ImmutableList.of(stringVal);
                    channelPublisher.publish(channelSet, value);

                    if (Thread.interrupted()) {
                        return;
                    }
                }
                thread1Finished.set(true);
            }
        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < messageAmount; i++) {
                    String stringVal = "value" + UUID.randomUUID();
                    uuidMap.put(stringVal, 0);
                    final List<String> value = ImmutableList.of(stringVal);
                    channelPublisher.publish(channelSet, value);

                    if (Thread.interrupted()) {
                        return;
                    }
                }
                thread2Finished.set(true);
            }
        });

        thread1.start();
        thread2.start();

        long timeToFinish = 1500;
        thread1.join(timeToFinish);
        thread2.join(timeToFinish);

        if (!thread1Finished.get() && !thread2Finished.get()) {
            fail("Did not finish in time");
        }

        final List<String> channels = ImmutableList.of("channel1", "channel2", "channel3", "channel4", "channel5");

        final List<Long> lastSeenIds = ImmutableList.of(0L, 0L, 0L, 0L, 0L);

        final ChannelReceiver channelReceiver = new ChannelLuaReceiver(rdbi);
        GetBulkResult result = channelReceiver.getMulti(channels, lastSeenIds);
        for (List<String> each : result.getMessages()) {
            for (String s : each) {
                if (uuidMap.get(s) != null) {
                    uuidMap.put(s, uuidMap.get(s) + 1);
                }
            }
        }

        channelPublisher.resetChannels(channelSet);

        for (String key : uuidMap.keySet()) {
            assertTrue(uuidMap.get(key) == channels.size());
        }

    }

    //ignored because this is a test to compare consecutive single channel gets vs. batch channel gets
    //results on a local redis instance
    //channels      batch(ms)   single(ms)
    //100	        15	        25
    //200	        20	        44
    //300	        40	        83
    //400	        44	        86
    //500	        75	        108
    //600	        61	        112
    //700           80	        128
    //800	        81	        158
    //900	        124	        170
    //1000       	118	        190
    //1100	        154	        206
    //1200	        142	        257
    //1300       	178	        302
    //1400       	159	        272
    //1500       	206	        320
    //1600       	183	        293
    //1700       	244	        332
    //1800       	217	        373
    //1900       	336	        476
    //2000       	310	        507
    @Test
    @Ignore
    public void luaReceiverBenchmarkTest() throws Exception {
        for (int channelsAmount = 100; channelsAmount <= 100; channelsAmount = channelsAmount + 100) {
            testRecieveChannelLuaTest2(channelsAmount, 1);
        }
    }

    private void testRecieveChannelLuaTest2(int channelsAmount, int threadAmount) throws InterruptedException {
        final String padding = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        final Set<String> channelsSet = new HashSet<>();
        final int numChannels = channelsAmount;
        final int messageAmount = 100;
        final int numThreads = threadAmount;
        final long timeToFinish = 200000;

        AtomicLong multiGetTime = new AtomicLong();
        AtomicLong singleGetTime = new AtomicLong();

        for (int i = 0; i < numChannels; i++) {
            channelsSet.add("channel" + i);
        }

        final List<String> channelsList = ImmutableList.copyOf(channelsSet);
        final List<Long> lastSeenIds = channelsList.stream().map(each -> 0L).collect(toList());

        List<Thread> threadList = new ArrayList<>(numThreads);
        List<AtomicBoolean> threadXFinished = new ArrayList();

        RDBI rdbi = new RDBI(new JedisPool("localhost"));

        final ChannelPublisher channelPublisher = new ChannelPublisher(rdbi);
        channelPublisher.resetChannels(channelsSet);

        final AtomicBoolean thread1Finished = new AtomicBoolean(false);

        //set up and populate channels
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < messageAmount; i++) {
                    String stringVal = "value" + i + padding;
                    final List<String> value = ImmutableList.of(stringVal);
                    channelPublisher.publish(channelsSet, value);

                    if (Thread.interrupted()) {
                        return;
                    }
                }
                thread1Finished.set(true);
            }
        });

        thread1.start();

        thread1.join(timeToFinish);

        if (!thread1Finished.get()) {
            fail("Did not finish in time");
        }

        for (int i = 0; i < numThreads * 2; i++) {
            threadXFinished.add(new AtomicBoolean(false));
        }

        //multiget
        for (int j = 0; j < numThreads; j++) {
            AtomicBoolean finished = threadXFinished.get(j);
            threadList.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    final ChannelReceiver channelReceiver2 = new ChannelLuaReceiver(rdbi);
                    for (int i = 0; i < 3; i++) {
                        channelReceiver2.getMulti(channelsList, lastSeenIds);
                    }

                    long oldTime = System.currentTimeMillis();
                    GetBulkResult result2 = channelReceiver2.getMulti(channelsList, lastSeenIds);
                    multiGetTime.set(System.currentTimeMillis() - oldTime);

                    System.out.println("Batch get time: " + multiGetTime.get() + " ms for " + channelsAmount + " channels");

                    if (Thread.interrupted()) {
                        return;
                    }

                    finished.set(true);
                }
            }));
        }

        //regular get
        for (int j = numThreads; j < numThreads * 2; j++) {
            AtomicBoolean finished = threadXFinished.get(j);
            threadList.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    final ChannelReceiver channelReceiver2 = new ChannelLuaReceiver(rdbi);
                    for (int i = 0; i < 3; i++) {
                        for (String channel : channelsList) {
                            channelReceiver2.get(channel, 0L);
                        }
                    }

                    long oldTime = System.currentTimeMillis();
                    for (String channel : channelsList) {
                        GetResult result = channelReceiver2.get(channel, 0L);
                    }
                    singleGetTime.set(System.currentTimeMillis() - oldTime);

                    System.out.println("single channel get time: " + singleGetTime.get() + " ms for " + channelsAmount + " channels");

                    if (Thread.interrupted()) {
                        return;
                    }

                    finished.set(true);
                }
            }));
        }

        for (Thread thred : threadList) {
            thred.start();
        }

        for (int i = 0; i < threadList.size(); i++) {
            threadList.get(i).join(timeToFinish);
            if (!threadXFinished.get(i).get()) {
                fail("Did not finish in time");
            }
        }

        channelPublisher.resetChannels(channelsSet);
    }
}
