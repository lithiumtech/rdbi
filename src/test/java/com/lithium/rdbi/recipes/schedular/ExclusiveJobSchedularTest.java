package com.lithium.rdbi.recipes.schedular;

import com.lithium.rdbi.RDBI;
import com.lithium.rdbi.recipes.schedular.ExclusiveJobSchedular;
import org.joda.time.Instant;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ExclusiveJobSchedularTest {

    @Test
    public void testBasicSchedule() throws InterruptedException {

        ExclusiveJobSchedular scheduledJobSystem = new ExclusiveJobSchedular(new RDBI(new JedisPool("localhost")), "myprefix:");
        scheduledJobSystem.nukeForTest("mytube");
        scheduledJobSystem.schedule("mytube", "{hello:world}", 1000);
        String result = scheduledJobSystem.reserve("mytube", 1000);
        assertNull(result);
        Thread.sleep(1000L);
        String result2 = scheduledJobSystem.reserve("mytube", 1000);
        assertEquals(result2, "{hello:world}");
        String result3 = scheduledJobSystem.reserve("mytube", 1000);
        assertNull(result3);

    }

    @Test
    public void testBasicPerformance() throws InterruptedException {
        ExclusiveJobSchedular scheduledJobSystem = new ExclusiveJobSchedular(new RDBI(new JedisPool("localhost")), "myprefix:");
        scheduledJobSystem.nukeForTest("mytube");

        Instant before = new Instant();
        for ( int i = 0; i < 10000; i++) {
            scheduledJobSystem.schedule("mytube", "{hello:world} " + i, 1000);
        }

        Instant after = new Instant();

        Thread.sleep(2000);

        System.out.println("final " + after.minus(before.getMillis()).getMillis());

        Instant before2 = new Instant();
        for ( int i = 0; i < 10000; i++) {
            scheduledJobSystem.reserve("mytube", 1000);
        }

        Instant after2 = new Instant();
        System.out.println("final " + after2.minus(before2.getMillis()).getMillis());
    }
}
