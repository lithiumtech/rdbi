package com.lithium.dbi.rdbi.recipes.scheduler;

import com.google.common.collect.ImmutableSet;
import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.testutil.TubeUtils;
import org.joda.time.Instant;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(groups = "integration")
public class TimeBasedJobSchedulerTest {

    private static final int QUIESCENCE = 1000;

    private static final RDBI rdbi = new RDBI(new JedisPool("localhost"));
    private String tubeName;

    private TimeBasedJobScheduler scheduledJobSystem = null;

    @BeforeMethod
    public void setup(){
        tubeName = TubeUtils.uniqueTubeName();
        scheduledJobSystem  = new TimeBasedJobScheduler(rdbi, "myprefix:");
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
    }

    @Test
    public void testBasicSchedule() throws InterruptedException {
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0, QUIESCENCE);
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");
        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result3);
    }

    @Test
    public void testDuplicateReserve() throws InterruptedException {
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0, QUIESCENCE);
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:world}", 0, QUIESCENCE));
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
    public void testRepeatSchedule() throws InterruptedException {
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:world}", 0, QUIESCENCE)); // set time to now
        assertTrue(scheduledJobSystem.schedule(tubeName, "{hello:world}", 500, QUIESCENCE)); // success ~500ms > original
        assertFalse(scheduledJobSystem.schedule(tubeName, "{hello:world}", 0, QUIESCENCE)); // fails < original
        assertFalse(scheduledJobSystem.schedule(tubeName, "{hello:world}", 1500, QUIESCENCE)); // fails ~1000ms > second
    }

    @Test
    public void testCull() throws InterruptedException {

        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0, QUIESCENCE);
        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNotNull(result3);
        Thread.sleep(2000);
        List<TimeJobInfo> infos = scheduledJobSystem.requeueExpired(tubeName);
        assertEquals(infos.size(), 1);
        assertNotNull(infos.get(0).getTime());
    }

    @Test
    public void testDelete() throws InterruptedException {
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0, QUIESCENCE);
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result2.getJobStr(), "{hello:world}");

        // Ready queue is empty since we already move it to the running queue.
        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertNull(result3);

        scheduledJobSystem.schedule(tubeName, "{hello:world}", 1000, QUIESCENCE);

        // Delete only applies to running...
        scheduledJobSystem.deleteRunningJob(tubeName, "{hello:world}");

        Thread.sleep(2000L);

        // So this delete did not do anything
        JobInfo result4 = scheduledJobSystem.reserveSingle(tubeName, 1000);
        assertEquals(result4.getJobStr(), "{hello:world}");
    }

    @Test
    public void testRequeue() throws InterruptedException {
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 0, QUIESCENCE);
        JobInfo result2 = scheduledJobSystem.reserveSingle(tubeName, 0);
        assertEquals(result2.getJobStr(), "{hello:world}");

        JobInfo result3 = scheduledJobSystem.reserveSingle(tubeName, 0);
        assertNull(result3);

        List<TimeJobInfo> requeued = scheduledJobSystem.requeueExpired(tubeName);
        assertEquals(requeued.size(), 1);
        assertEquals(requeued.get(0).getJobStr(), "{hello:world}");

        JobInfo result4 = scheduledJobSystem.reserveSingle(tubeName, 0);
        assertEquals(result4.getJobStr(), "{hello:world}");
    }

    @Test
    public void testBasicStates() throws InterruptedException {
        scheduledJobSystem.schedule(tubeName, "{hello:world}", 1000, QUIESCENCE);

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

        scheduledJobSystem.deleteRunningJob(tubeName, "{hello:world}");
        assertEquals(scheduledJobSystem.peekDelayed(tubeName, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peekReady(tubeName, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peekRunning(tubeName, 1, 0).size(), 0);
        assertEquals(scheduledJobSystem.peekExpired(tubeName, 1, 0).size(), 0);
    }

    @Test
    public void testBasicPerformance() throws InterruptedException {

        Instant before = new Instant();
        for ( int i = 0; i < 10000; i++) {
            scheduledJobSystem.schedule(tubeName, "{hello:world} " + i, 0, QUIESCENCE);
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
    public void testBasicScheduleMulti() throws InterruptedException {
        final String payloadFoo = "{hello:foo}";
        final String payloadBar = "{hello:bar}";
        final String payloadBaz = "{hello:baz}";

        assertEquals(0, scheduledJobSystem.scheduleMulti(tubeName, ImmutableSet.<String>of(), 0));

        assertEquals(1, scheduledJobSystem.scheduleMulti(tubeName, ImmutableSet.of(payloadFoo), 0));
        assertEquals(0, scheduledJobSystem.scheduleMulti(tubeName, ImmutableSet.of(payloadFoo), 0));

        assertEquals(2, scheduledJobSystem.scheduleMulti(tubeName, ImmutableSet.of(payloadFoo, payloadBar, payloadBaz, payloadBar, payloadBaz), 0));

        List<TimeJobInfo> jobInfos = scheduledJobSystem.reserveMulti(tubeName, 1000, 5);
        assertEquals(3, jobInfos.size());
    }

    @Test
    public void testScheduleMultiPlaysNicelyWithQuiescence() throws InterruptedException {
        final String payloadFoo = "{hello:foo}";
        final String payloadBar = "{hello:bar}";
        assertEquals(2, scheduledJobSystem.scheduleMulti(tubeName, ImmutableSet.of(payloadFoo, payloadBar), 0));
        assertFalse(scheduledJobSystem.schedule(tubeName, payloadFoo, 10, 0));
        assertTrue(scheduledJobSystem.schedule(tubeName, payloadFoo, 10, QUIESCENCE));
    }

}
