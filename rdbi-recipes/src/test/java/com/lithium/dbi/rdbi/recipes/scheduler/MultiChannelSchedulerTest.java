package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.TestClock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "integration")
public class MultiChannelSchedulerTest {
    private static final RDBI rdbi = new RDBI(new JedisPool("localhost"));
    private static final String prefix = "mc-test:";
    private String tube1 = "tube1";
    private String channel1 = "channel1";

    @AfterMethod
    public void tearDown() {
        try (Handle handle = rdbi.open()) {
            handle.jedis().keys(prefix + "*")
                  .forEach(key -> handle.jedis().del(key));
        }
    }

    /**
     * simple test of a single tube / channel job combo
     */
    @Test
    public void basicTest() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);

        String jobId = channel1 + ":" + tube1 + "_1";
        boolean schedule1 = scheduledJobSystem.schedule(channel1, tube1, jobId, 0);

        // should not be scheduled
        boolean schedule2 = scheduledJobSystem.schedule(channel1, tube1, jobId, 0);

        assertThat(schedule1).isTrue();
        assertThat(schedule2).isFalse();

        List<String> running = scheduledJobSystem.getAllReadyChannels(tube1);
        assertThat(running)
                .hasSize(1)
                .contains(prefix + ":" + channel1 + ":" + tube1);

        List<TimeJobInfo> job1 = scheduledJobSystem.reserveMulti(tube1, 1000, 1);
        List<TimeJobInfo> job2 = scheduledJobSystem.reserveMulti(tube1, 1000, 1);

        assertThat(job1)
                .hasSize(1);

        assertThat(job1.get(0).getJobStr())
                .isEqualTo(jobId);

        assertThat(job2).isEmpty();

        boolean ack1 = scheduledJobSystem.ackJob(tube1, job1.get(0).getJobStr());

        assertThat(ack1).isTrue();
    }

    @Test
    public void multiChannelFairnessTest() {
        // have 3 companies
        // submit 10 jobs for A
        // submit 5 for B
        // submit 2 for C
        // under old scheduler, we'd drain these in order of submission
        // now we expect to get [ A, B, C, A, B, C, A, B, A, B, A, B, A, A, A, A, A ] if no other jobs are submitted
        // further, when we submit a new C when 3 As remain, C will run when 2 As remain, regardless of original submission time

        TestClock clock = new TestClock(System.currentTimeMillis() - 30, 1L);

        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix, clock);


        boolean allScheduled = IntStream.rangeClosed(1, 10).mapToObj(i -> {
            clock.tick(); // make sure time passes between each submitted job
            String jobId = "A" + ":" + tube1 + "_" + i;
            return scheduledJobSystem.schedule("A", tube1, jobId, 0);
        }).allMatch(scheduled -> scheduled);

        assertThat(allScheduled).isTrue();

        allScheduled = IntStream.rangeClosed(1, 5).mapToObj(i -> {
            clock.tick(); // make sure time passes between each submitted job
            String jobId = "B" + ":" + tube1 + "_" + i;
            return scheduledJobSystem.schedule("B", tube1, jobId, 0);
        }).allMatch(scheduled -> scheduled);
        assertThat(allScheduled).isTrue();

        clock.tick(); // make sure time passes between each submitted job
        String jobId = "C" + ":" + tube1 + "_bleh";
        assertThat(scheduledJobSystem.schedule("C", tube1, jobId, 0)).isTrue();

        assertThat(scheduledJobSystem.getReadyJobCount("A", tube1)).isEqualTo(10);
        assertThat(scheduledJobSystem.getReadyJobCount("B", tube1)).isEqualTo(5);
        assertThat(scheduledJobSystem.getReadyJobCount("C", tube1)).isEqualTo(1);


        final Consumer<String> reserveAndAssert = channel -> {
            List<TimeJobInfo> job1 = scheduledJobSystem.reserveMulti(tube1, 1000, 1);
            assertThat(job1)
                    .hasSize(1);
            assertThat(job1.get(0).getJobStr())
                    .startsWith(channel);

        };

        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .containsExactlyInAnyOrder(prefix + ":A:" + tube1, prefix + ":B:" + tube1, prefix + ":C:" + tube1);

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("B");
        reserveAndAssert.accept("C");

        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .containsExactlyInAnyOrder(prefix + ":A:" + tube1, prefix + ":B:" + tube1);

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("B");

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("B");

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("B");

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("B");

        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .containsExactlyInAnyOrder(prefix + ":A:" + tube1);

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("A");

        // now we have only A jobs with 3 remaining.
        // if we schedule a C job now, it should run
        // before all the A jobs finish

        clock.tick(); // make sure time passes between each submitted job
        jobId = "C" + ":" + tube1 + "_bleh2";
        assertThat(scheduledJobSystem.schedule("C", tube1, jobId, 0)).isTrue();

        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .containsExactlyInAnyOrder(prefix + ":A:" + tube1, prefix + ":C:" + tube1);

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("C");

        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .containsExactlyInAnyOrder(prefix + ":A:" + tube1);

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("A");

        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .isEmpty();

        assertThat(scheduledJobSystem.getRunningJobCount(tube1)).isEqualTo(17);
    }

    @Test
    public void testRemoveExpiredRunning() {

        TestClock clock = new TestClock(System.currentTimeMillis(), 20L);
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix, clock);
        String jobId = channel1 + ":" + tube1;

        assertThat(scheduledJobSystem.schedule(channel1, tube1, jobId, 0)).isTrue();

        scheduledJobSystem.reserveMulti(tube1, 10L, 1);

        assertThat(scheduledJobSystem.peekExpired(tube1, 0, 1)).isEmpty();

        // pre-tick, removing expired has no effect
        assertThat(scheduledJobSystem.removeExpiredRunningJobs(tube1)).isEmpty();

        // ticking forward 20ms will put this job as expired
        clock.tick();
        assertThat(scheduledJobSystem.peekExpired(tube1, 0, 1)).hasSize(1);

        assertThat(scheduledJobSystem.removeExpiredRunningJobs(tube1)).hasSize(1);
        assertThat(scheduledJobSystem.peekRunning(tube1, 0, 1)).isEmpty();
    }

    @Test
    public void testRemoveExpiredReady() {
        TestClock clock = new TestClock(System.currentTimeMillis(), 20L);
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix, clock);
        String jobId = channel1 + ":" + tube1;

        assertThat(scheduledJobSystem.schedule(channel1, tube1, jobId, 10)).isTrue();

        // not expired yet
        assertThat(scheduledJobSystem.removeExpiredReadyJobs(channel1, tube1, clock.getAsLong())).isEmpty();

        clock.tick();
        assertThat(scheduledJobSystem.removeExpiredReadyJobs(channel1, tube1, clock.getAsLong())).hasSize(1);

        // ready queue now empty
        assertThat(scheduledJobSystem.peekReady(channel1, tube1, 0, 1)).isEmpty();
    }

    @Test
    public void testPauseCannotSchedule() {
        TestClock clock = new TestClock(System.currentTimeMillis(), 1L);
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix, clock);

        String jobId = channel1 + ":" + tube1;

        scheduledJobSystem.pause(channel1, tube1);
        assertThat(scheduledJobSystem.isPaused(channel1, tube1)).isTrue();

        assertThat(scheduledJobSystem.getPauseStart(channel1, tube1)).isEqualTo(String.valueOf(clock.getAsLong() / 1000));

        assertThat(scheduledJobSystem.schedule(channel1, tube1, jobId, 0)).isFalse();
        assertThat(scheduledJobSystem.inReadyQueue(channel1, tube1, jobId)).isFalse();

        scheduledJobSystem.resume(channel1, tube1);

        assertThat(scheduledJobSystem.schedule(channel1, tube1, jobId, 0)).isTrue();
        assertThat(scheduledJobSystem.inReadyQueue(channel1, tube1, jobId)).isTrue();
    }

    @Test
    public void testPauseCannotReserve() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        String jobId = channel1 + ":" + tube1;

        assertThat(scheduledJobSystem.schedule(channel1, tube1, jobId, 0)).isTrue();

        scheduledJobSystem.pause(channel1, tube1);
        assertThat(scheduledJobSystem.isPaused(channel1, tube1)).isTrue();

        // can't reserve when paused
        List<TimeJobInfo> jobs = scheduledJobSystem.reserveMulti(tube1, 1000L, 1);
        assertThat(jobs).hasSize(0);

        scheduledJobSystem.resume(channel1, tube1);

        // now i can reserve
        jobs = scheduledJobSystem.reserveMulti(tube1, 1000L, 1);
        assertThat(jobs).hasSize(1);
    }

    @Test
    public void testPeekDelayed() {
        TestClock clock = new TestClock(System.currentTimeMillis(), 2L);
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix, clock);
        String jobId = channel1 + ":" + tube1;

        assertThat(scheduledJobSystem.schedule(channel1, tube1, jobId, 1)).isTrue();

        // since the clock hasn't ticked it is still delayed
        List<TimeJobInfo> delayed = scheduledJobSystem.peekDelayed(channel1, tube1, 0, 10);
        assertThat(delayed).hasSize(1);

        clock.tick();
        // but now it should be delayed
        delayed = scheduledJobSystem.peekDelayed(channel1, tube1, 0, 10);
        assertThat(delayed).isEmpty();
    }

    @Test
    public void testPeekReady() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        String jobId = channel1 + ":" + tube1;

        assertThat(scheduledJobSystem.peekReady(channel1, tube1, 0, 10)).hasSize(0);

        scheduledJobSystem.schedule(channel1, tube1, jobId + "_1", 0);
        scheduledJobSystem.schedule(channel1, tube1, jobId + "_2", 0);

        assertThat(scheduledJobSystem.peekReady(channel1, tube1, 0, 10)).hasSize(2);
        assertThat(scheduledJobSystem.peekReady(channel1, tube1, 0, 1)).hasSize(1);

    }

    @Test
    public void testPeekRunning() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        String jobId = channel1 + ":" + tube1;

        scheduledJobSystem.schedule(channel1, tube1, jobId + "_1", 0);
        scheduledJobSystem.schedule(channel1, tube1, jobId + "_2", 0);

        scheduledJobSystem.reserveMulti(tube1, 1000L, 2);
        assertThat(scheduledJobSystem.peekReady(channel1, tube1, 0, 10)).hasSize(0);
        assertThat(scheduledJobSystem.peekRunning(tube1, 0, 10)).hasSize(2);
    }

    @Test
    public void testPeekExpired() {

        TestClock clock = new TestClock(System.currentTimeMillis(), 20L);
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix, clock);
        String jobId = channel1 + ":" + tube1;

        assertThat(scheduledJobSystem.schedule(channel1, tube1, jobId, 0)).isTrue();

        scheduledJobSystem.reserveMulti(tube1, 10L, 1);

        assertThat(scheduledJobSystem.peekExpired(tube1, 0, 1)).isEmpty();

        // ticking forward 20ms will put this job as expired
        clock.tick();
        assertThat(scheduledJobSystem.peekExpired(tube1, 0, 1)).hasSize(1);
    }

    @Test
    public void testInReadyQueue() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        String jobId = channel1 + ":" + tube1 + "_1";
        scheduledJobSystem.schedule(channel1, tube1, jobId, 0);
        assertThat(scheduledJobSystem.inReadyQueue(channel1, tube1, jobId)).isTrue();
    }

    @Test
    public void testInRunningQueue() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        String jobId = channel1 + ":" + tube1 + "_1";
        scheduledJobSystem.schedule(channel1, tube1, jobId, 0);
        scheduledJobSystem.reserveMulti(tube1, 1000, 1);
        assertThat(scheduledJobSystem.inRunningQueue(tube1, jobId)).isTrue();
    }
}
