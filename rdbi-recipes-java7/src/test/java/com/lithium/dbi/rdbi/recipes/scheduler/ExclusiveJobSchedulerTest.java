package com.lithium.dbi.rdbi.recipes.scheduler;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;
import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.testutil.TubeUtils;
import org.joda.time.Instant;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import javax.annotation.Nullable;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(groups = "integration")
public class ExclusiveJobSchedulerTest {

    private static final RDBI rdbi = new RDBI(new JedisPool("localhost"));
    private String tubeName;

    private ExclusiveJobScheduler scheduledJobSystem = null;

    @BeforeMethod
    public void setup(){
        tubeName = TubeUtils.uniqueTubeName();
        scheduledJobSystem  = new ExclusiveJobScheduler(rdbi, "myprefix:");
    }

    @AfterMethod
    public void tearDown(){
        // nuke the queues
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().del(scheduledJobSystem.getReadyQueue(tubeName), scheduledJobSystem.getRunningQueue(tubeName));
                return null;
            }
        });

        // ensure system is not paused
        scheduledJobSystem.resume(tubeName);
    }

    @Test
    public void testBasicSchedule() throws InterruptedException {

        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0);
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");
        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result3);
    }

    @Test
    public void testCull() throws InterruptedException {

        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0);
        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNotNull(result3);
        Thread.sleep(2000);
        List<TimeJobInfo> infos = scheduledJobSystem.removeExpiredJobs(tubeName);
        assertEquals(infos.size(), 1);
        assertNotNull(infos.get(0).getJobScore());
    }

    @Test
    public void testDelete() throws InterruptedException {
        assertFalse(scheduledJobSystem.deleteJob(tubeName, "{hello:world}"));

        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0);
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");

        //while in the running queue
        assertTrue(scheduledJobSystem.deleteJob(tubeName, result2.getJobStr()));
        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result3);

        scheduledJobSystem.schedule(tubeName, "{hello:world}", 1000);

        //while in ready queue
        assertTrue(scheduledJobSystem.deleteJob(tubeName, "{hello:world}"));

        Thread.sleep(2000L);

        JobInfo result4 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result4);
    }

    @Test
    public void testBasicStates() throws InterruptedException {
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 1000);

        List<TimeJobInfo> jobInfos = scheduledJobSystem.peekDelayed(tubeName, 0, 1);
        assertEquals(jobInfos.get(0).getJobStr(), "{hello:world}");

        Thread.sleep(1500);
        List<TimeJobInfo> jobInfos2 = scheduledJobSystem.peekDelayed(tubeName, 0, 1);
        assertEquals(jobInfos2.size(), 0);

        List<TimeJobInfo> jobInfos3 = scheduledJobSystem.peekReady(tubeName, 0, 1);
        assertEquals(jobInfos3.get(0).getJobStr(), "{hello:world}");

        scheduledJobSystem.reserveSingle(tubeName, 1000L);
        List<TimeJobInfo> jobInfos4 = scheduledJobSystem.peekRunning(tubeName, 0, 1);
        assertEquals(jobInfos4.get(0).getJobStr(), "{hello:world}");

        Thread.sleep(1500);
        List<TimeJobInfo> jobInfos6 = scheduledJobSystem.peekRunning(tubeName, 0, 1);
        assertEquals(jobInfos6.size(), 0);

        List<TimeJobInfo> jobInfos5 = scheduledJobSystem.peekExpired(tubeName, 0, 1);
        assertEquals(jobInfos5.get(0).getJobStr(), "{hello:world}");

        scheduledJobSystem.deleteJob(tubeName,  "{hello:world}");
        assertEquals(scheduledJobSystem.peekDelayed(tubeName, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peekReady(tubeName, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peekRunning(tubeName, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peekExpired(tubeName, 1, 0).size(), 0);
    }

    @Test
    public void testBasicPerformance() throws InterruptedException {

        Instant before = new Instant();
        for ( int i = 0; i < 10000; i++) {
            scheduledJobSystem.schedule(tubeName, "{hello:world} " + i, 0);
        }
        Instant after = new Instant();
        System.out.println("final " + after.minus(before.getMillis()).getMillis());

        Thread.sleep(2000);

        Instant before2 = new Instant();
        for ( int i = 0; i < 10000; i++) {
            scheduledJobSystem.reserveSingle(tubeName, 1);
        }

        Instant after2 = new Instant();
        System.out.println("final " + after2.minus(before2.getMillis()).getMillis());
    }

    @Test
    public void testIsPaused(){
        assertFalse(scheduledJobSystem.isPaused(tubeName));
        scheduledJobSystem.pause(tubeName);
        assertTrue(scheduledJobSystem.isPaused(tubeName));
        scheduledJobSystem.resume(tubeName);
        assertFalse(scheduledJobSystem.isPaused(tubeName));
    }

    @Test
    public void testPause(){
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:delayed}", 10000), "PRE-CONDITION: failed to create 1 delayed job");
        // scheduling a "ready" job with a ttl of 0 means that it may come back when we make the call to peekDelayed
        // unless we force at least a millisecond delay.  Rather than sleep the thread, let's just schedule this to run
        // sometime in the past, making it "overdue" and thus ready
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:ready}", -10000), "PRE-CONDITION: failed to create 1 ready job");


        scheduledJobSystem.pause(tubeName);

        assertFalse(scheduledJobSystem.schedule(tubeName, "{hello:world}", 0), "Should not be able to schedule anything when the system is paused.");
        JobInfo result = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result, "Should not have been able to reserve a job while the system is paused");
    }

    @Test
    public void testResume(){
        scheduledJobSystem.pause(tubeName);
        assertTrue(scheduledJobSystem.isPaused(tubeName), "PRE-CONDITION: system should be paused.");

        scheduledJobSystem.resume(tubeName);
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:delayed}", 10000), "Failed to schedule a delayed job after resuming the system.");
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:ready}", -10000), "Failed to schedule a ready job after resuming the system.");
        JobInfo result = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNotNull(result, "Should have been able to reserve a job while the system is paused");
    }

    @Test
    public void testReadyExpiration() throws InterruptedException {

        // Create two jobs that were scheduled to run in the past and have been sitting
        // around for more than 500 ms.
        scheduledJobSystem.schedule(tubeName, "{job1}", -1000);
        scheduledJobSystem.schedule(tubeName, "{job2}", -1500);
        // Create a job that is ready to run but not considered expired.
        scheduledJobSystem.schedule(tubeName, "{job3}", 0);

        // Find all expired jobs = jobs that have been sitting in the ready queue for more than 500 millis
        List<TimeJobInfo> jobInfos = scheduledJobSystem.removeExpiredReadyJobs(tubeName, 500);
        assertEquals(jobInfos.size(), 2);
        List<String> jobStrList = FluentIterable.from(jobInfos).transform(new Function<TimeJobInfo, String>() {
            @Nullable
            @Override
            public String apply(TimeJobInfo timeJobInfo) {
                return timeJobInfo.getJobStr();
            }
        }).toList();

        assertTrue(jobStrList.containsAll(Sets.newHashSet("{job1}", "{job2}")));

        // Wait for job3 to become available and confirm its still there.
        List<TimeJobInfo> reservedList = scheduledJobSystem.reserveMulti(tubeName, 10000, 3);
        assertEquals(reservedList.size(), 1);
        assertEquals(reservedList.get(0).getJobStr(), "{job3}");
    }

    @Test
    public void testReadyRunningCount() {
        scheduledJobSystem.schedule(tubeName, "{job1}", 0);
        scheduledJobSystem.schedule(tubeName, "{job2}", 0);
        scheduledJobSystem.schedule(tubeName, "{job3}", 0);

        assertEquals(scheduledJobSystem.getReadyJobCount(tubeName), 3);
        assertEquals(scheduledJobSystem.getRunningJobCount(tubeName), 0);

        scheduledJobSystem.reserveSingle(tubeName, 10000);

        assertEquals(scheduledJobSystem.getReadyJobCount(tubeName), 2);
        assertEquals(scheduledJobSystem.getRunningJobCount(tubeName), 1);

        scheduledJobSystem.reserveSingle(tubeName, 10000);
        assertEquals(scheduledJobSystem.getReadyJobCount(tubeName), 1);
        assertEquals(scheduledJobSystem.getRunningJobCount(tubeName), 2);

        scheduledJobSystem.reserveSingle(tubeName, 10000);
        assertEquals(scheduledJobSystem.getReadyJobCount(tubeName), 0);
        assertEquals(scheduledJobSystem.getRunningJobCount(tubeName), 3);
    }

    @Test
    public void testReadyCountWithSomeJobsNotReady() throws Exception {
        scheduledJobSystem.schedule(tubeName, "{job1}", 0);
        scheduledJobSystem.schedule(tubeName, "{job2}", 500);
        scheduledJobSystem.schedule(tubeName, "{job3}", 10000000);

        assertEquals(scheduledJobSystem.getReadyJobCount(tubeName), 1);

        scheduledJobSystem.reserveSingle(tubeName, 10000);

        assertEquals(scheduledJobSystem.getReadyJobCount(tubeName), 0);

        Thread.sleep(500);

        assertEquals(scheduledJobSystem.getReadyJobCount(tubeName), 1);

        scheduledJobSystem.reserveSingle(tubeName, 10000);
        assertEquals(scheduledJobSystem.getReadyJobCount(tubeName), 0);
    }
}
