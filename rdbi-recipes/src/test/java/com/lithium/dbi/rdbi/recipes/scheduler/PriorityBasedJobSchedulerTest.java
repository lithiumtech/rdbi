package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.testutil.TubeUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.time.Instant;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class PriorityBasedJobSchedulerTest {

    private static final RDBI rdbi = new RDBI(new JedisPool("localhost"));
    private String tubeName;

    private PriorityBasedJobScheduler scheduledJobSystem = null;

    @BeforeMethod
    public void setup(){
        tubeName = TubeUtils.uniqueTubeName();
        scheduledJobSystem  = new PriorityBasedJobScheduler(rdbi, "myprefix:");
    }

    @AfterMethod
    public void tearDown(){
        // nuke the queues
        rdbi.withHandle((Callback<Void>) handle -> {
            handle.jedis().del(scheduledJobSystem.getReadyQueue(tubeName), scheduledJobSystem.getRunningQueue(tubeName));
            return null;
        });
    }

    @Test
    public void testBasicSchedule() throws InterruptedException {
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 3.00);
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");
        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result3);
    }

    @Test
    public void testDuplicateReserve() throws InterruptedException {
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 3.00);
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:world}", 3.00));
        // The same case now exists in ready and running.
        // Reservation should fail...
        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result3);
        // Remove from running & reserve again should succeed
        scheduledJobSystem.deleteRunningJob(tubeName, "{hello:world}");
        JobInfo result4 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result4.getJobStr(), "{hello:world}");
    }

    @Test
    public void testCull() throws InterruptedException {
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 3.00);
        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNotNull(result3);
        Thread.sleep(2000);
        List<JobInfo> infos = scheduledJobSystem.requeueExpired(tubeName, 1.00);
        assertEquals(infos.size(), 1);
        assertNotNull(infos.get(0).getJobScore());
    }

    @Test
    public void testDelete() throws InterruptedException {
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 3.00);
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");

        // Ready queue is empty since we already move it to the running queue.
        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result3);

        scheduledJobSystem.schedule(tubeName, "{hello:world}", 3.00);

        // Delete only applies to running...
        scheduledJobSystem.deleteRunningJob(tubeName, "{hello:world}");

        Thread.sleep(2000L);

        // Ensure delete did not do anything
        JobInfo result4 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result4.getJobStr(), "{hello:world}");
    }

    @Test
    public void testRequeue() throws InterruptedException {
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 3.00);
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 0);
        assertEquals(result2.getJobStr(), "{hello:world}");

        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 0);
        assertNull(result3);

        List<JobInfo> requeued = scheduledJobSystem.requeueExpired(tubeName, 1.00);
        assertEquals(requeued.size(), 1);
        assertEquals(requeued.get(0).getJobStr(), "{hello:world}");

        JobInfo result4 = scheduledJobSystem.reserveSingle(tubeName, 0);
        assertEquals(result4.getJobStr(), "{hello:world}");
    }

    @Test
    public void testBasicPerformance() throws InterruptedException {

        Instant before = Instant.now();
        for ( int i = 0; i < 10000; i++) {
            scheduledJobSystem.schedule(tubeName, "{hello:world} " + i, 3.00);
        }
        Instant after = Instant.now();
        System.out.println("final " + after.minusMillis(before.toEpochMilli()).toEpochMilli());

        Thread.sleep(2000);

        Instant before2 = Instant.now();
        for ( int i = 0; i < 10000; i++) {
            scheduledJobSystem.reserveSingle(tubeName, 1);
        }

        Instant after2 = Instant.now();
        System.out.println("final " + after2.minusMillis(before2.toEpochMilli()).toEpochMilli());
    }

}
