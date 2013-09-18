package com.lithium.dbi.rdbi.recipes.channel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.AssertJUnit.fail;

@Test(groups = "integration")
public class ChannelPublisherTest {

    @Test
    public void testPublishChannelPerformanceTest() throws InterruptedException {

        final Set<String> channel = ImmutableSet.of("channel1", "channel2", "channel3", "channel4", "channel5");

        final ChannelPublisher channelPublisher = new ChannelPublisher(new RDBI(new JedisPool("localhost")));
        channelPublisher.resetChannels(channel);

        final List<String> value =ImmutableList.of("value1");
        final AtomicBoolean thread1Finished = new AtomicBoolean(false);
        final AtomicBoolean thread2Finished = new AtomicBoolean(false);

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for ( int i = 0; i < 1000; i++) {
                    channelPublisher.publish(channel, value );

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
                for ( int i = 0; i < 1000; i++) {
                    channelPublisher.publish(channel, value );
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
    }
}
