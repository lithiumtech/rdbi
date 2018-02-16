package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.Instant;
import java.util.Set;

@Test(groups = "integration")
public class ExclusiveJobSchedulerPerformanceTest {

    @Test
    public void testPerformance() throws InterruptedException {

        RDBI rdbi = new RDBI(new JedisPool("localhost"));

        final ExclusiveJobScheduler scheduledJobSystem = new ExclusiveJobScheduler(rdbi, "myprefix:");

        for ( int i = 0; i < 100; i++) {
            scheduledJobSystem.schedule("mytube", "{hello:world} " + i, 0);
        }

        for ( int i = 0; i < 100; i++) {
            scheduledJobSystem.reserveSingle("mytube", 1);
        }

        Handle handle = rdbi.open();

        try {
            Jedis jedis = handle.jedis();
            Set<String> results = jedis.keys("*");
            jedis.del((String[]) results.toArray(new String[0]));
        } finally {
            handle.close();
        }

        Instant before = Instant.now();

        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 3333; i++) {
                scheduledJobSystem.schedule("mytube", "{hello:world} " + i, 0);
            }
        });

        Thread t2 = new Thread(() -> {
            for (int i = 0; i < 3333; i++) {
                scheduledJobSystem.schedule("mytube", "{hello:world} " + i, 0);
            }
        });

        Thread t3 = new Thread(() -> {
            for (int i = 0; i < 3333; i++) {
                scheduledJobSystem.schedule("mytube", "{hello:world} " + i, 0);
            }
        });


        t1.start();
        t2.start();
        t3.start();

        t3.join();
        t1.join();
        t2.join();

        Instant after = Instant.now();

        Thread.sleep(2000);

        System.out.println("final " + after.minusMillis(before.toEpochMilli()).toEpochMilli());

        Instant before2 = Instant.now();
        for (int i = 0; i < 10000; i++) {
            scheduledJobSystem.reserveSingle("mytube", 1);
        }

        Instant after2 = Instant.now();
        System.out.println("final " + after2.minusMillis(before2.toEpochMilli()).toEpochMilli());
    }
}
