package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test(groups = "integration")
public class JobSchedulerTest {

    private static final RDBI rdbi = new RDBI(new JedisPool("localhost"));
    private static final String TEST_TUBE = "mytube";

    private JobScheduler scheduledJobSystem = null;

    @BeforeMethod
    public void setup(){
        scheduledJobSystem  = new JobScheduler(rdbi, "myprefix:");
    }

    @AfterMethod
    public void tearDown(){
        // nuke the queues
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().del(scheduledJobSystem.getReadyQueue(TEST_TUBE), scheduledJobSystem.getRunningQueue(TEST_TUBE));
                return null;
            }
        });
    }

    @Test
    public void testBasicSchedule() throws InterruptedException {

        scheduledJobSystem.schedule(TEST_TUBE, "{hello:world}", 0);
        JobInfo result2 = scheduledJobSystem.reserveSingle(TEST_TUBE, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");
        JobInfo result3 = scheduledJobSystem.reserveSingle(TEST_TUBE, 1000);
        assertNull(result3);
    }

    @Test
    public void testCull() throws InterruptedException {

        scheduledJobSystem.schedule(TEST_TUBE, "{hello:world}", 0);
        JobInfo result3 = scheduledJobSystem.reserveSingle(TEST_TUBE, 1000);
        assertNotNull(result3);
        Thread.sleep(2000);
        List<JobInfo> infos = scheduledJobSystem.requeueExpired(TEST_TUBE);
        assertEquals(infos.size(), 1);
        assertNotNull(infos.get(0).getTime());
    }

    @Test
    public void testDelete() throws InterruptedException {
        scheduledJobSystem.schedule(TEST_TUBE, "{hello:world}", 0);
        JobInfo result2 = scheduledJobSystem.reserveSingle(TEST_TUBE, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");

        // Ready queue is empty since we already move it to the running queue.
        JobInfo result3 = scheduledJobSystem.reserveSingle(TEST_TUBE, 1000);
        assertNull(result3);

        scheduledJobSystem.schedule(TEST_TUBE, "{hello:world}", 1000);

        // Delete only applies to running...
        scheduledJobSystem.deleteRunningJob(TEST_TUBE, "{hello:world}");

        Thread.sleep(2000L);

        // So this delete did not do anything
        JobInfo result4 = scheduledJobSystem.reserveSingle(TEST_TUBE, 1000);
        assertEquals(result4.getJobStr(), "{hello:world}");
    }

    @Test
    public void testRequeue() throws InterruptedException {
        scheduledJobSystem.schedule(TEST_TUBE, "{hello:world}", 0);
        JobInfo result2 = scheduledJobSystem.reserveSingle(TEST_TUBE, 0);
        assertEquals(result2.getJobStr(), "{hello:world}");

        JobInfo result3 = scheduledJobSystem.reserveSingle(TEST_TUBE, 0);
        assertNull(result3);

        List<JobInfo> requeued = scheduledJobSystem.requeueExpired(TEST_TUBE);
        assertEquals(requeued.size(), 1);
        assertEquals(requeued.get(0).getJobStr(), "{hello:world}");

        JobInfo result4 = scheduledJobSystem.reserveSingle(TEST_TUBE, 0);
        assertEquals(result4.getJobStr(), "{hello:world}");
    }

    @Test
    public void testBasicStates() throws InterruptedException {
        scheduledJobSystem.schedule(TEST_TUBE, "{hello:world}", 1000);

        List<JobInfo> jobInfos = scheduledJobSystem.peekDelayed(TEST_TUBE, 0, 1);
        assertEquals(jobInfos.get(0).getJobStr(), "{hello:world}");

        Thread.sleep(1500);
        List<JobInfo> jobInfos2 = scheduledJobSystem.peekDelayed(TEST_TUBE, 0, 1);
        assertEquals(jobInfos2.size(), 0);

        List<JobInfo> jobInfos3 = scheduledJobSystem.peekReady(TEST_TUBE, 0, 1);
        assertEquals(jobInfos3.get(0).getJobStr(), "{hello:world}");

        scheduledJobSystem.reserveSingle("mytube", 1000L);
        List<JobInfo> jobInfos4 = scheduledJobSystem.peekRunning(TEST_TUBE, 0, 1);
        assertEquals(jobInfos4.get(0).getJobStr(), "{hello:world}");

        Thread.sleep(1500);
        List<JobInfo> jobInfos6 = scheduledJobSystem.peekRunning(TEST_TUBE, 0, 1);
        assertEquals(jobInfos6.size(), 0);

        List<JobInfo> jobInfos5 = scheduledJobSystem.peekExpired(TEST_TUBE, 0, 1);
        assertEquals(jobInfos5.get(0).getJobStr(), "{hello:world}");

        scheduledJobSystem.deleteRunningJob(TEST_TUBE, "{hello:world}");
        assertEquals(scheduledJobSystem.peekDelayed(TEST_TUBE, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peekReady(TEST_TUBE, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peekRunning(TEST_TUBE, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peekExpired(TEST_TUBE, 1, 0).size(), 0);
    }

    @Test
    public void testBasicPerformance() throws InterruptedException {

        Instant before = new Instant();
        for ( int i = 0; i < 10000; i++) {
            scheduledJobSystem.schedule(TEST_TUBE, "{hello:world} " + i, 0);
        }
        Instant after = new Instant();
        System.out.println("final " + after.minus(before.getMillis()).getMillis());

        Thread.sleep(2000);

        Instant before2 = new Instant();
        for ( int i = 0; i < 10000; i++) {
            scheduledJobSystem.reserveSingle(TEST_TUBE, 1);
        }

        Instant after2 = new Instant();
        System.out.println("final " + after2.minus(before2.getMillis()).getMillis());
    }

}
