package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.testutil.TubeUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "integration")
public class StateDedupedJobSchedulerTest {
    private static final RDBI rdbi = new RDBI(new JedisPool("localhost"));
    private String tubeName;

    private StateDedupedJobScheduler scheduledJobSystem = null;

    @BeforeMethod
    public void setup(){
        tubeName = TubeUtils.uniqueTubeName();
        scheduledJobSystem  = new StateDedupedJobScheduler(rdbi, "myprefix:");
    }

    @AfterMethod
    public void tearDown(){
        // nuke the queues
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().del(scheduledJobSystem.getReadyQueue(tubeName),
                                   scheduledJobSystem.getRunningQueue(tubeName),
                                   scheduledJobSystem.getReadyAndRunningQueue(tubeName));
                return null;
            }
        });

        // ensure system is not paused
        scheduledJobSystem.resume(tubeName);
    }

    @Test
    public void testBasicSchedule() {
        // Schedule a job.
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0);

        // Reserve the job. Should succeed
        JobInfo result1 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result1.getJobStr(), "{hello:world}");

        // Try and reserve another job. Since nothing else to reserve should get back a null result.
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result2);
    }

    @Test
    public void testBasicScheduleWhenPaused() {
        scheduledJobSystem.pause(tubeName);

        // Schedule a job. Should not be able to since the tube is paused.
        assertFalse(scheduledJobSystem.schedule(tubeName, "{hello:world}", 0));
    }

    @Test
    public void testScheduleSameJobMultipleTimesWithoutReserving() {
        // Schedule a job.
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:world}", 0));

        // Schedule same job again and verify it doesn't get scheduled since the job is already in ready queue.
        assertFalse(scheduledJobSystem.schedule(tubeName, "{hello:world}", 0));
    }

    @Test
    public void testReserveWhenPaused() {
        // Schedule a job
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0);

        // Pause the tube.
        scheduledJobSystem.pause(tubeName);

        // Reserve the job. Should not be able to since the tube is paused.
        JobInfo result1 = scheduledJobSystem.reserveSingle(tubeName, 500);
        assertNull(result1);
    }


    @Test
    public void testScheduleJobReserveAndScheduleAgain() {
        // Schedule a job
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:world}", 0));

        // Reserve the job.
        JobInfo result = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNotNull(result);

        // Schedule same job again. Should be able to since the other instance of the job has been
        // reserved - is in running queue.
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:world}", 0));

        // Shouldn't be able to reserve the second instance since we still have the first instance
        // in the running queue.
        result = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result);

        // Ack the first instance of the job.
        scheduledJobSystem.ackJob(tubeName, "{hello:world}");

        // Should be able to reserve second instance now that the first instance has been completed/acked.
        result = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNotNull(result);
    }

    @Test
    public void testMultiReservationWhenOneJobInReadyAndRunning() {
        // Schedule a job
        assertTrue(scheduledJobSystem.schedule(tubeName, "job1", -5000));
        // Reserve the job so its in the running queue.
        scheduledJobSystem.reserveSingle(tubeName, 1000);

        // Schedule same job again so its in the ready queue as well.
        assertTrue(scheduledJobSystem.schedule(tubeName, "job1", -3000));

        // Schedule a few jobs that are ready to be reserved and one that is not.
        assertTrue(scheduledJobSystem.schedule(tubeName, "job2", -3000));
        assertTrue(scheduledJobSystem.schedule(tubeName, "job3", 898789000));
        assertTrue(scheduledJobSystem.schedule(tubeName, "job4", -3000));
        assertTrue(scheduledJobSystem.schedule(tubeName, "job5", -2000));

        // Reserve 2 jobs. Should return job2 and job4 since they are the next jobs ready to be reserved.
        // job1 can't be reserved since its in the running queue when this reservation request is made and
        // so it should be ignored.
        List<TimeJobInfo> reservedJobs = scheduledJobSystem.reserveMulti(tubeName, 1000, 2);

        boolean job2Reserved = false;
        boolean job4Reserved = false;
        for(TimeJobInfo jobInfo : reservedJobs) {
            if(jobInfo.getJobStr().equals("job2")) {
                job2Reserved = true;
            } else if(jobInfo.getJobStr().equals("job4")) {
                job4Reserved = true;
            } else {
                fail("Reserved an unexpected job: " + jobInfo.getJobStr());
            }
        }
        assertTrue(job2Reserved);
        assertTrue(job4Reserved);
    }

    @Test
    public void testMultiReservations() {
        // Schedule a job
        assertTrue(scheduledJobSystem.schedule(tubeName, "job1", -5000));
        // Reserve the job so its in the running queue.
        scheduledJobSystem.reserveSingle(tubeName, 1000);

        // Schedule another job.
        assertTrue(scheduledJobSystem.schedule(tubeName, "job2", -5000));
        // Reserve the job so its in the running queue.
        scheduledJobSystem.reserveSingle(tubeName, 1000);

        assertTrue(scheduledJobSystem.schedule(tubeName, "job1", -10000));  // Should not be able to reserve since its in running queue
        assertTrue(scheduledJobSystem.schedule(tubeName, "job3", -6000));   // Should be able to reserve first iteration
        assertTrue(scheduledJobSystem.schedule(tubeName, "job5", -5500));   // Should be able to reserve after job 3 during first iteration
        assertTrue(scheduledJobSystem.schedule(tubeName, "job2", -5000));   // Should not be able to reserve since its in the running queue
        assertTrue(scheduledJobSystem.schedule(tubeName, "job6", -2000));   // Should be able to reserve after job 5 during second iteration
        assertTrue(scheduledJobSystem.schedule(tubeName, "job4", 898789000)); // Not ready to be reserved yet.

        // Reserve 3 jobs. Should return job3, job5 and job6 since they are the next jobs ready to be reserved.
        // job1 and job2 can't be reserved since they are in the running queue when this reservation request is made
        List<TimeJobInfo> reservedJobs = scheduledJobSystem.reserveMulti(tubeName, 1000, 3);

        boolean job3Reserved = false;
        boolean job5Reserved = false;
        boolean job6Reserved = false;
        for(TimeJobInfo jobInfo : reservedJobs) {
            if(jobInfo.getJobStr().equals("job3")) {
                job3Reserved = true;
            } else if(jobInfo.getJobStr().equals("job5")) {
                job5Reserved = true;
            } else if(jobInfo.getJobStr().equals("job6")) {
                job6Reserved = true;
            } else {
                fail("Reserved an unexpected job: " + jobInfo.getJobStr());
            }
        }
        assertTrue(job3Reserved);
        assertTrue(job5Reserved);
        assertTrue(job6Reserved);
    }

    @Test
    public void testMultiReservationsWithLessAvailableThanLimit() {
        // Schedule a job
        assertTrue(scheduledJobSystem.schedule(tubeName, "job1", -5000));
        // Reserve the job so its in the running queue.
        scheduledJobSystem.reserveSingle(tubeName, 1000);

        // Schedule another job.
        assertTrue(scheduledJobSystem.schedule(tubeName, "job2", -5000));
        // Reserve the job so its in the running queue.
        scheduledJobSystem.reserveSingle(tubeName, 1000);

        assertTrue(scheduledJobSystem.schedule(tubeName, "job1", -10000));  // Should not be able to reserve since its in running queue
        assertTrue(scheduledJobSystem.schedule(tubeName, "job3", -6000));   // Should be able to reserve first iteration
        assertTrue(scheduledJobSystem.schedule(tubeName, "job2", -5000));   // Should not be able to reserve since its in the running queue
        assertTrue(scheduledJobSystem.schedule(tubeName, "job4", 898789000)); // Not ready to be reserved yet.

        // Reserve 3 jobs. Should return job3 its the next job ready to be reserved.
        // job1 and job2 can't be reserved since they are the running queue when this reservation request is made.
        List<TimeJobInfo> reservedJobs = scheduledJobSystem.reserveMulti(tubeName, 1000, 3);

        assertEquals(reservedJobs.size(), 1);
        assertEquals(reservedJobs.get(0).getJobStr(), "job3");
    }

    @Test
    public void testDelete() throws InterruptedException {
        final String jobStr = "{hello:world}";
        assertFalse(scheduledJobSystem.deleteJob(tubeName, jobStr));

        //////////////// Test deleting from the running queue.
        // Schedule and reserve job.
        scheduledJobSystem.schedule(tubeName, jobStr, 0);
        JobInfo result1 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result1.getJobStr(), jobStr);

        // Delete the job thats in the running queue.
        assertTrue(scheduledJobSystem.deleteJob(tubeName, result1.getJobStr()));

        // Verify you cant reserve any jobs.
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result2);


        /////////////// Test deleting from ready queue.
        // Schedule the job again.
        scheduledJobSystem.schedule(tubeName, jobStr, 0);

        //Delete job while its in the ready queue.
        assertTrue(scheduledJobSystem.deleteJob(tubeName, jobStr));

        // Try to reserve and shouldn't be able to.
        JobInfo result4 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result4);

        ///////////// Test deleting from the running and ready queue.
        // Schedule job again.
        scheduledJobSystem.schedule(tubeName, jobStr, 0);

        // Reserve job.
        JobInfo result5 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result5.getJobStr(), jobStr);

        // Schedule job again so its in the ready and running queue at the same time.
        assertTrue(scheduledJobSystem.schedule(tubeName, jobStr, 0));

        // assert readiness
        assertFalse(scheduledJobSystem.inReadyQueue(tubeName, jobStr));
        assertTrue(scheduledJobSystem.inReadyAndRunningQueue(tubeName, jobStr));
        assertTrue(scheduledJobSystem.inRunningQueue(tubeName, jobStr));


        assertTrue(scheduledJobSystem.deleteJobFromReady(tubeName, jobStr));
        assertFalse(scheduledJobSystem.inReadyQueue(tubeName, jobStr));
        assertFalse(scheduledJobSystem.inReadyAndRunningQueue(tubeName, jobStr));
        assertTrue(scheduledJobSystem.inRunningQueue(tubeName, jobStr));

        //Delete job while its in the ready and running queue.
        assertTrue(scheduledJobSystem.deleteJob(tubeName, jobStr));

        assertFalse(scheduledJobSystem.inReadyQueue(tubeName, jobStr));
        assertFalse(scheduledJobSystem.inRunningQueue(tubeName, jobStr));

        // Try to reserve and shouldn't be able to.
        JobInfo result6 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result6);

    }

    @Test
    public void testAck() throws InterruptedException {
        // Schedule.
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:world}", 0));

        // Verify the job is in the ready queue.
        assertTrue(scheduledJobSystem.inReadyQueue(tubeName, "{hello:world}"));
        assertFalse(scheduledJobSystem.inRunningQueue(tubeName, "{hello:world}"));

        // Attempt to ack non running job. Should fail
        assertFalse(scheduledJobSystem.ackJob(tubeName, "{hello:world}"));

        JobInfo result1 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result1.getJobStr(), "{hello:world}");

        // Verify the job is no longer in the ready queue.
        assertFalse(scheduledJobSystem.inReadyQueue(tubeName, "{hello:world}"));
        assertTrue(scheduledJobSystem.inRunningQueue(tubeName, "{hello:world}"));

        // Schedule the job again. Since its now reserved should be able to schedule it again.
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:world}", 0));

        // Verify the job is in the ready-and-running queue now.
        assertFalse(scheduledJobSystem.inReadyQueue(tubeName, "{hello:world}"));
        assertTrue(scheduledJobSystem.inReadyAndRunningQueue(tubeName, "{hello:world}"));
        assertTrue(scheduledJobSystem.inRunningQueue(tubeName, "{hello:world}"));

        // Ack the job.
        assertTrue(scheduledJobSystem.ackJob(tubeName, "{hello:world}"));

        // Verify the job is now in ready queue even after the ack, but not ready and running
        assertTrue(scheduledJobSystem.inReadyQueue(tubeName, "{hello:world}"));
        assertFalse(scheduledJobSystem.inReadyAndRunningQueue(tubeName, "{hello:world}"));
        assertFalse(scheduledJobSystem.inRunningQueue(tubeName, "{hello:world}"));

        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");
    }

    @Test
    public void testRemoveExpiredRunningJobs() throws InterruptedException {
        // Schedule a job
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0);

        // Reserve the job.
        JobInfo result1 = scheduledJobSystem.reserveSingle(tubeName, 1);
        assertNotNull(result1);

        // Sleep enough time to allow it to expire.
        Thread.sleep(10);

        // Verify the job is returned as expired.
        List<TimeJobInfo> infos = scheduledJobSystem.removeExpiredRunningJobs(tubeName);
        assertEquals(infos.size(), 1);
        assertNotNull(infos.get(0).getJobScore());
    }

    @Test
    public void testRemoveExpiredReadyJobs() throws InterruptedException {
        // Schedule a job
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0);

        // Sleep enough time to allow it to expire.
        Thread.sleep(50);

        // Verify the job is returned as expired.
        List<TimeJobInfo> infos = scheduledJobSystem.removeExpiredReadyJobs(tubeName, 10);
        assertEquals(infos.size(), 1);
        assertNotNull(infos.get(0).getJobScore());
    }

    @Test
    public void testRemoveExpiredReadyAndRunningJobs() throws InterruptedException {
        // Schedule a job
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0);
        // move it to running
        JobInfo result1 = scheduledJobSystem.reserveSingle(tubeName, 1000);

        // schedule again
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0);

        // Sleep enough time to allow it to expire.
        Thread.sleep(50);

        // Verify the job is returned as expired.
        List<TimeJobInfo> infos = scheduledJobSystem.removeExpiredReadyJobs(tubeName, 10);
        assertEquals(infos.size(), 1);
        assertNotNull(infos.get(0).getJobScore());

        assertFalse(scheduledJobSystem.inReadyQueue(tubeName, "{hello:world}"));
        assertFalse(scheduledJobSystem.inReadyAndRunningQueue(tubeName, "{hello:world}"));
        assertTrue(scheduledJobSystem.inRunningQueue(tubeName, "{hello:world}"));
        // Ack the job.
        assertTrue(scheduledJobSystem.ackJob(tubeName, "{hello:world}"));
    }

    @Test
    public void testReadyJobCountWhenReadyAndRunning() {
        // Schedule a job
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0);
        // move it to running
        scheduledJobSystem.reserveSingle(tubeName, 1000);

        // schedule again
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0);

        // we should count it as 'ready'
        assertThat(scheduledJobSystem.getReadyJobCount(tubeName)).isEqualTo(1);
    }
}
