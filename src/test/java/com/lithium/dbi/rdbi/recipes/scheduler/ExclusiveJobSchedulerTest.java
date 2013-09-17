package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test(groups = "integration")
public class ExclusiveJobSchedulerTest {

    @Test
    public void testBasicSchedule() throws InterruptedException {

        ExclusiveJobScheduler scheduledJobSystem = new ExclusiveJobScheduler(new RDBI(new JedisPool("localhost")), "myprefix:");
        scheduledJobSystem.nukeForTest("mytube");
        scheduledJobSystem.schedule("mytube", "{hello:world}", 0);
        JobInfo result2 = scheduledJobSystem.reserveSingle("mytube", 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");
        JobInfo result3 = scheduledJobSystem.reserveSingle("mytube", 1000);
        assertNull(result3);
    }

    @Test
    public void testCull() throws InterruptedException {

        ExclusiveJobScheduler scheduledJobSystem = new ExclusiveJobScheduler(new RDBI(new JedisPool("localhost")), "myprefix:");
        scheduledJobSystem.nukeForTest("mytube");

        scheduledJobSystem.schedule("mytube", "{hello:world}", 0);
        JobInfo result3 = scheduledJobSystem.reserveSingle("mytube", 1000);
        assertNotNull(result3);
        Thread.sleep(2000);
        List<JobInfo> infos = scheduledJobSystem.removeExpiredJobs("mytube");
        assertEquals(infos.size(), 1);
        assertNotNull(infos.get(0).getTime());
    }

    @Test
    public void testDelete() throws InterruptedException {
        ExclusiveJobScheduler scheduledJobSystem = new ExclusiveJobScheduler(new RDBI(new JedisPool("localhost")), "myprefix:");
        scheduledJobSystem.nukeForTest("mytube");
        scheduledJobSystem.schedule("mytube", "{hello:world}", 0);
        JobInfo result2 = scheduledJobSystem.reserveSingle("mytube", 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");

        //while in the running queue
        scheduledJobSystem.deleteJob("mytube", result2.getJobStr());
        JobInfo result3 = scheduledJobSystem.reserveSingle("mytube", 1000);
        assertNull(result3);

        scheduledJobSystem.schedule("mytube", "{hello:world}", 1000);

        //while in ready queue
        scheduledJobSystem.deleteJob("mytube", "{hello:world}");

        Thread.sleep(2000L);

        JobInfo result4 = scheduledJobSystem.reserveSingle("mytube", 1000);
        assertNull(result4);
    }

    @Test
    public void testBasicStates() throws InterruptedException {
        ExclusiveJobScheduler scheduledJobSystem = new ExclusiveJobScheduler(new RDBI(new JedisPool("localhost")), "myprefix:");
        scheduledJobSystem.nukeForTest("mytube");
        scheduledJobSystem.schedule("mytube", "{hello:world}", 1000);

        List<JobInfo> jobInfos = scheduledJobSystem.peakDelayed("mytube", 0, 1 );
        assertEquals(jobInfos.get(0).getJobStr(), "{hello:world}");

        Thread.sleep(1500);
        List<JobInfo> jobInfos2 = scheduledJobSystem.peakDelayed("mytube", 0, 1);
        assertEquals(jobInfos2.size(), 0);

        List<JobInfo> jobInfos3 = scheduledJobSystem.peakReady("mytube", 0, 1);
        assertEquals(jobInfos3.get(0).getJobStr(), "{hello:world}");

        scheduledJobSystem.reserveSingle("mytube", 1000L);
        List<JobInfo> jobInfos4 = scheduledJobSystem.peakRunning("mytube", 0, 1);
        assertEquals(jobInfos4.get(0).getJobStr(), "{hello:world}");

        Thread.sleep(1500);
        List<JobInfo> jobInfos6 = scheduledJobSystem.peakRunning("mytube", 0, 1);
        assertEquals(jobInfos6.size(), 0);

        List<JobInfo> jobInfos5 = scheduledJobSystem.peakExpired("mytube", 0, 1);
        assertEquals(jobInfos5.get(0).getJobStr(), "{hello:world}");

        scheduledJobSystem.deleteJob("mytube",  "{hello:world}");
        assertEquals(scheduledJobSystem.peakDelayed("mytube", 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peakReady("mytube", 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peakRunning("mytube", 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peakExpired("mytube", 1, 0).size(), 0);
    }

    @Test
    public void testBasicPerformance() throws InterruptedException {

        ExclusiveJobScheduler scheduledJobSystem = new ExclusiveJobScheduler(new RDBI(new JedisPool("localhost")), "myprefix:");
        scheduledJobSystem.nukeForTest("mytube");

        Instant before = new Instant();
        for ( int i = 0; i < 10000; i++) {
            scheduledJobSystem.schedule("mytube", "{hello:world} " + i, 0);
        }
        Instant after = new Instant();
        System.out.println("final " + after.minus(before.getMillis()).getMillis());

        Thread.sleep(2000);

        Instant before2 = new Instant();
        for ( int i = 0; i < 10000; i++) {
            scheduledJobSystem.reserveSingle("mytube", 1);
        }

        Instant after2 = new Instant();
        System.out.println("final " + after2.minus(before2.getMillis()).getMillis());
    }
}
