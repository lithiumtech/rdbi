package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test(groups = "integration")
public class ExclusiveJobSchedulerTest {

    private static final RDBI rdbi = new RDBI(new JedisPool("localhost"));
    private static final String TEST_TUBE = "mytube";

    private ExclusiveJobScheduler scheduledJobSystem = null;

    @BeforeMethod
    public void setup(){
        scheduledJobSystem  = new ExclusiveJobScheduler(rdbi, "myprefix:");
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

        // ensure system is not paused
        scheduledJobSystem.resume(TEST_TUBE);
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
        List<JobInfo> infos = scheduledJobSystem.removeExpiredJobs(TEST_TUBE);
        assertEquals(infos.size(), 1);
        assertNotNull(infos.get(0).getTime());
    }

    @Test
    public void testDelete() throws InterruptedException {
        scheduledJobSystem.schedule(TEST_TUBE, "{hello:world}", 0);
        JobInfo result2 = scheduledJobSystem.reserveSingle(TEST_TUBE, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");

        //while in the running queue
        scheduledJobSystem.deleteJob(TEST_TUBE, result2.getJobStr());
        JobInfo result3 = scheduledJobSystem.reserveSingle(TEST_TUBE, 1000);
        assertNull(result3);

        scheduledJobSystem.schedule(TEST_TUBE, "{hello:world}", 1000);

        //while in ready queue
        scheduledJobSystem.deleteJob(TEST_TUBE, "{hello:world}");

        Thread.sleep(2000L);

        JobInfo result4 = scheduledJobSystem.reserveSingle(TEST_TUBE, 1000);
        assertNull(result4);
    }

    @Test
    public void testBasicStates() throws InterruptedException {
        scheduledJobSystem.schedule(TEST_TUBE, "{hello:world}", 1000);

        List<JobInfo> jobInfos = scheduledJobSystem.peakDelayed(TEST_TUBE, 0, 1 );
        assertEquals(jobInfos.get(0).getJobStr(), "{hello:world}");

        Thread.sleep(1500);
        List<JobInfo> jobInfos2 = scheduledJobSystem.peakDelayed(TEST_TUBE, 0, 1);
        assertEquals(jobInfos2.size(), 0);

        List<JobInfo> jobInfos3 = scheduledJobSystem.peakReady(TEST_TUBE, 0, 1);
        assertEquals(jobInfos3.get(0).getJobStr(), "{hello:world}");

        scheduledJobSystem.reserveSingle("mytube", 1000L);
        List<JobInfo> jobInfos4 = scheduledJobSystem.peakRunning(TEST_TUBE, 0, 1);
        assertEquals(jobInfos4.get(0).getJobStr(), "{hello:world}");

        Thread.sleep(1500);
        List<JobInfo> jobInfos6 = scheduledJobSystem.peakRunning(TEST_TUBE, 0, 1);
        assertEquals(jobInfos6.size(), 0);

        List<JobInfo> jobInfos5 = scheduledJobSystem.peakExpired(TEST_TUBE, 0, 1);
        assertEquals(jobInfos5.get(0).getJobStr(), "{hello:world}");

        scheduledJobSystem.deleteJob(TEST_TUBE,  "{hello:world}");
        assertEquals(scheduledJobSystem.peakDelayed(TEST_TUBE, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peakReady(TEST_TUBE, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peakRunning(TEST_TUBE, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peakExpired(TEST_TUBE, 1, 0).size(), 0);
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

    @Test
    public void testIsPaused(){
        assertFalse(scheduledJobSystem.isPaused(TEST_TUBE));
        scheduledJobSystem.pause(TEST_TUBE);
        assertTrue(scheduledJobSystem.isPaused(TEST_TUBE));
        scheduledJobSystem.resume(TEST_TUBE);
        assertFalse(scheduledJobSystem.isPaused(TEST_TUBE));
    }

    @Test
    public void testPause(){
        scheduledJobSystem.schedule(TEST_TUBE, "{hello:delayed}", 10000);
        // scheduling a "ready" job with a ttl of 0 means that it may come back when we make the call to peekDelayed
        // unless we force at least a millisecond delay.  Rather than sleep the thread, let's just schedule this to run
        // sometime in the past, making it "overdue" and thus ready
        scheduledJobSystem.schedule(TEST_TUBE, "{hello:ready}", -10000);

        List<JobInfo> jobs = scheduledJobSystem.peekDelayed(TEST_TUBE, 0, 10);
        assertEquals(jobs.get(0).getJobStr(), "{hello:delayed}", "PRE-CONDITION: failed to create 1 delayed job");
        jobs = scheduledJobSystem.peekReady(TEST_TUBE, 0, 10);
        assertEquals(jobs.get(0).getJobStr(), "{hello:ready}", "PRE-CONDITION: failed to create 1 ready job");

        scheduledJobSystem.pause(TEST_TUBE);

        assertFalse(scheduledJobSystem.schedule(TEST_TUBE, "{hello:world}", 0), "Should not be able to schedule anything when the system is stopped.");
        JobInfo result = scheduledJobSystem.reserveSingle(TEST_TUBE, 1000);
        assertNull(result, "Should not have been able to reserve a job while the system is paused");
    }

    @Test
    public void testResume(){
        scheduledJobSystem.pause(TEST_TUBE);
        assertFalse(scheduledJobSystem.schedule(TEST_TUBE, "{hello:world}", 0), "PRE-CONDITION: system should be stopped.");

        scheduledJobSystem.resume(TEST_TUBE);
        assertTrue(scheduledJobSystem.schedule(TEST_TUBE, "{hello:delayed}", 10000), "Failed to schedule a delayed job after starting the system.");
        assertTrue(scheduledJobSystem.schedule(TEST_TUBE, "{hello:ready}", -10000), "Failed to schedule a ready job after starting the system.");
        JobInfo result = scheduledJobSystem.reserveSingle(TEST_TUBE, 1000);
        assertNotNull(result, "Should have been able to reserve a job while the system is paused");
    }

}
