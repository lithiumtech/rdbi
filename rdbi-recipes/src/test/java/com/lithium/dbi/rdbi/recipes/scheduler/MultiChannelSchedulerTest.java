package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.TestClock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@Test(groups = "integration")
public class MultiChannelSchedulerTest {
    private static final RDBI rdbi = new RDBI(new JedisPool("localhost", 6379));
    private static final String prefix = "mc-test:";
    private String tube1 = "tube1";
    private String channel1 = "channel1";

    private final ScheduleReader scheduleReader = new ScheduleReader(rdbi, prefix);

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
        scheduledJobSystem.enablePerChannelTracking();

        String jobId = channel1 + ":" + tube1 + "_1";
        boolean schedule1 = scheduledJobSystem.schedule(channel1, tube1, jobId, 0);

        // should not be scheduled
        boolean schedule2 = scheduledJobSystem.schedule(channel1, tube1, jobId, 0);

        assertThat(schedule1).isTrue();
        assertThat(schedule2).isFalse();

        List<String> running = scheduledJobSystem.getAllReadyChannels(tube1);
        assertThat(running)
                .hasSize(1)
                .contains(channel1);

        List<TimeJobInfo> job1 = scheduledJobSystem.reserveMulti(tube1, 1000, 1);
        List<TimeJobInfo> job2 = scheduledJobSystem.reserveMulti(tube1, 1000, 1);

        assertThat(job1)
                .hasSize(1);

        assertThat(job1.get(0).getJobStr())
                .isEqualTo(jobId);

        assertThat(job2).isEmpty();

        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(1);
        boolean ack1 = scheduledJobSystem.ackJob(channel1, tube1, job1.get(0).getJobStr());

        assertThat(ack1).isTrue();
        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);
        ack1 = scheduledJobSystem.ackJob(channel1, tube1, job1.get(0).getJobStr());
        assertThat(ack1).isFalse();
        // no negative
        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);

    }

    @Test
    public void testPerChannelTrackingToggles() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        assertThat(scheduledJobSystem.isPerChannelTrackingEnabled()).isFalse();


        String jobId = channel1 + ":" + tube1 + "_1";
        scheduledJobSystem.schedule(channel1, tube1, jobId, 0);
        scheduledJobSystem.reserveMulti(tube1, 1000, 1);

        // we aren't tracking yet
        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);

        // now enable
        assertThat(scheduledJobSystem.enablePerChannelTracking()).isTrue();
        assertThat(scheduledJobSystem.isPerChannelTrackingEnabled()).isTrue();
        // if we tried to enable again, we get a false but are still enabled
        assertThat(scheduledJobSystem.enablePerChannelTracking()).isFalse();
        assertThat(scheduledJobSystem.isPerChannelTrackingEnabled()).isTrue();

        // we don't automagically track things that were already running
        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);

        scheduledJobSystem.ackJob(channel1, tube1, jobId);

        // even though we're tracking, negatives can't happen
        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);

        scheduledJobSystem.schedule(channel1, tube1, jobId, 0);
        scheduledJobSystem.reserveMulti(tube1, 1000, 1);

        // we are tracking now
        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(1);

        // now disable
        assertThat(scheduledJobSystem.disablePerChannelTracking()).isTrue();
        assertThat(scheduledJobSystem.isPerChannelTrackingEnabled()).isFalse();
        // if we tried to disable again we get false but still disabled
        assertThat(scheduledJobSystem.disablePerChannelTracking()).isFalse();
        assertThat(scheduledJobSystem.isPerChannelTrackingEnabled()).isFalse();

        // we can't automagically untrack things
        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(1);


        scheduledJobSystem.ackJob(channel1, tube1, jobId);

        // even though we're not tracking anymore we still decrement on exit
        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);
    }

    @Test
    public void testPerChannelTrackingNotNegative() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        scheduledJobSystem.enablePerChannelTracking();

        String jobId = channel1 + ":" + tube1 + "_1";
        scheduledJobSystem.schedule(channel1, tube1, jobId, 0);
        scheduledJobSystem.reserveMulti(tube1, 1000, 1);

        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(1);

        // artificially decrement
        rdbi.withHandle(h -> h.jedis().decr(prefix + ":" + channel1 + ":" + tube1 + ":running_count"));

        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);

        scheduledJobSystem.ackJob(channel1, tube1, jobId);
        // not negative
        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);
    }

    @Test
    public void testPerChannelTrackingKeyRemoval() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        scheduledJobSystem.enablePerChannelTracking();

        String jobId = channel1 + ":" + tube1 + "_1";
        String jobId2 = channel1 + ":" + tube1 + "_2";

        scheduledJobSystem.schedule(channel1, tube1, jobId, 0);
        scheduledJobSystem.reserveMulti(tube1, 1000, 1);

        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(1);

        scheduledJobSystem.schedule(channel1, tube1, jobId2, 0);
        scheduledJobSystem.reserveMulti(tube1, 1000, 1);

        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(2);

        scheduledJobSystem.ackJob(channel1, tube1, jobId);

        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(1);

        scheduledJobSystem.ackJob(channel1, tube1, jobId2);

        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);
        boolean exists = rdbi.withHandle(h -> h.jedis().exists(prefix + ":" + channel1 + ":" + tube1 + ":running_count"));
        assertThat(exists).isFalse();
    }

    @Test
    public void testPerChannelTrackingNotNegative2() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        scheduledJobSystem.enablePerChannelTracking();

        String jobId = channel1 + ":" + tube1 + "_1";
        scheduledJobSystem.schedule(channel1, tube1, jobId, 0);
        scheduledJobSystem.reserveMulti(tube1, 1000, 1);

        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(1);

        // artificially remove
        rdbi.withHandle(h -> h.jedis().del(prefix + ":" + channel1 + ":" + tube1 + ":running_count"));

        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);

        scheduledJobSystem.ackJob(channel1, tube1, jobId);
        // not negative
        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);
    }


    // test reserve multi works within a single channel.
    // it's a known limitation that it doesn't work across channels
    @Test
    public void testReserveMulti() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);

        String jobId = "ajob:" + tube1;
        scheduledJobSystem.schedule(channel1, tube1, jobId + "_1", 0);
        scheduledJobSystem.schedule(channel1, tube1, jobId + "_2", 0);
        scheduledJobSystem.schedule(channel1, tube1, jobId + "_3", 0);

        assertThat(scheduleReader.getAllReadyJobCount(tube1)).isEqualTo(3);

        List<TimeJobInfo> infos = scheduledJobSystem.reserveMulti(tube1, 1_000L, 3);

        assertThat(infos).hasSize(3);
        assertThat(scheduleReader.getAllReadyJobCount(tube1)).isEqualTo(0);
        assertThat(scheduleReader.getRunningJobCount(tube1)).isEqualTo(3);
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
        scheduledJobSystem.enablePerChannelTracking();

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

        assertThat(scheduleReader.getReadyJobCount("A", tube1)).isEqualTo(10);
        assertThat(scheduleReader.getReadyJobCount("B", tube1)).isEqualTo(5);
        assertThat(scheduleReader.getReadyJobCount("C", tube1)).isEqualTo(1);

        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(0);
        assertThat(scheduleReader.getRunningCountForChannel("B", tube1)).isEqualTo(0);
        assertThat(scheduleReader.getRunningCountForChannel("C", tube1)).isEqualTo(0);

        final Consumer<String> reserveAndAssert = channel -> {
            List<TimeJobInfo> job1 = scheduledJobSystem.reserveMulti(tube1, 1000, 1);
            assertThat(job1)
                    .hasSize(1);
            assertThat(job1.get(0).getJobStr())
                    .startsWith(channel);

        };

        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .containsExactlyInAnyOrder("A", "B", "C");

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("B");
        reserveAndAssert.accept("C");

        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(1);
        assertThat(scheduleReader.getRunningCountForChannel("B", tube1)).isEqualTo(1);
        assertThat(scheduleReader.getRunningCountForChannel("C", tube1)).isEqualTo(1);


        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .containsExactlyInAnyOrder("A", "B");

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("B");

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("B");

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("B");

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("B");

        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(5);
        assertThat(scheduleReader.getRunningCountForChannel("B", tube1)).isEqualTo(5);
        assertThat(scheduleReader.getRunningCountForChannel("C", tube1)).isEqualTo(1);


        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .containsExactlyInAnyOrder("A");

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("A");

        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(7);
        assertThat(scheduleReader.getRunningCountForChannel("B", tube1)).isEqualTo(5);
        assertThat(scheduleReader.getRunningCountForChannel("C", tube1)).isEqualTo(1);

        // now we have only A jobs with 3 remaining.
        // if we schedule a C job now, it should run
        // before all the A jobs finish

        clock.tick(); // make sure time passes between each submitted job
        jobId = "C" + ":" + tube1 + "_bleh2";
        assertThat(scheduledJobSystem.schedule("C", tube1, jobId, 0)).isTrue();

        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .containsExactlyInAnyOrder("A", "C");

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("C");

        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(8);
        assertThat(scheduleReader.getRunningCountForChannel("B", tube1)).isEqualTo(5);
        assertThat(scheduleReader.getRunningCountForChannel("C", tube1)).isEqualTo(2);

        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .containsExactlyInAnyOrder("A");

        reserveAndAssert.accept("A");
        reserveAndAssert.accept("A");

        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(10);
        assertThat(scheduleReader.getRunningCountForChannel("B", tube1)).isEqualTo(5);
        assertThat(scheduleReader.getRunningCountForChannel("C", tube1)).isEqualTo(2);

        assertThat(scheduledJobSystem.getAllReadyChannels(tube1))
                .isEmpty();

        assertThat(scheduleReader.getRunningJobCount(tube1)).isEqualTo(17);

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
    public void testRemoveExpiredRunningAndDecrement() {

        Function<TimeJobInfo, String> tjiToChannel = tji -> tji.jobStr.split(":")[0];

        TestClock clock = new TestClock(System.currentTimeMillis(), 20L);
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix, clock);
        scheduledJobSystem.enablePerChannelTracking();

        String jobId = channel1 + ":" + tube1;

        assertThat(scheduledJobSystem.schedule(channel1, tube1, jobId, 0)).isTrue();

        scheduledJobSystem.reserveMulti(tube1, 10L, 1);

        assertThat(scheduledJobSystem.peekExpired(tube1, 0, 1)).isEmpty();

        // pre-tick, removing expired has no effect
        assertThat(scheduledJobSystem.removeExpiredRunningJobs(tube1)).isEmpty();

        // ticking forward 20ms will put this job as expired
        clock.tick();
        assertThat(scheduledJobSystem.peekExpired(tube1, 0, 1)).hasSize(1);

        assertThat(scheduledJobSystem.removeExpiredRunningJobsAndDecrementCount(tube1, tjiToChannel)).hasSize(1);

        assertThat(scheduleReader.getRunningCountForChannel(channel1, tube1)).isEqualTo(0);
        boolean exists = rdbi.withHandle(h -> h.jedis().exists(prefix + ":" + channel1 + ":" + tube1 + ":running_count"));
        assertThat(exists).isFalse();
    }

    @Test
    public void testUpdateTTL() {

        TestClock clock = new TestClock(System.currentTimeMillis(), 20L);
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix, clock);
        String jobId = channel1 + ":" + tube1;

        assertThat(scheduledJobSystem.schedule(channel1, tube1, jobId, 0)).isTrue();

        scheduledJobSystem.reserveMulti(tube1, 10L, 1);

        assertThat(scheduledJobSystem.peekExpired(tube1, 0, 1)).isEmpty();

        // pre-tick, removing expired has no effect
        assertThat(scheduledJobSystem.removeExpiredRunningJobs(tube1)).isEmpty();


        // update our ttl
        boolean incremented = scheduledJobSystem.incrementRunningTTL(channel1, tube1, jobId, 20);
        assertThat(incremented).isTrue();

        // ticking forward 20ms would have put this job as expired
        clock.tick();
        assertThat(scheduledJobSystem.peekExpired(tube1, 0, 1)).isEmpty();

        // ticking forward 20ms will now put this job as expired
        clock.tick();

        assertThat(scheduledJobSystem.removeExpiredRunningJobs(tube1)).hasSize(1);
        assertThat(scheduledJobSystem.peekRunning(tube1, 0, 1)).isEmpty();
    }

    @Test
    public void testUpdateTTLNotExists() {

        TestClock clock = new TestClock(System.currentTimeMillis(), 20L);
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix, clock);
        String jobId = channel1 + ":" + tube1;
        // update our ttl
        boolean incremented = scheduledJobSystem.incrementRunningTTL(channel1, tube1, jobId, 20);
        // nothing happened
        assertThat(incremented).isFalse();
        // we didn't create anything
        assertThat(scheduledJobSystem.peekRunning(tube1, 0, 1)).isEmpty();
    }

    @Test
    public void testRemoveExpiredReady() {
        TestClock clock = new TestClock(System.currentTimeMillis(), 20L);
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix, clock);
        String jobId = "yo:" + tube1;

        assertThat(scheduledJobSystem.schedule("A", tube1, jobId + "_1", 10)).isTrue();
        assertThat(scheduledJobSystem.schedule("B", tube1, jobId + "_2", 10)).isTrue();
        assertThat(scheduledJobSystem.schedule("C", tube1, jobId + "_3", 10)).isTrue();

        assertThat(scheduledJobSystem.schedule("D", tube1, jobId + "_1a", 30)).isTrue();
        assertThat(scheduledJobSystem.schedule("E", tube1, jobId + "_2a", 30)).isTrue();
        assertThat(scheduledJobSystem.schedule("F", tube1, jobId + "_3a", 30)).isTrue();

        // not expired yet
        assertThat(scheduledJobSystem.removeExpiredReadyJobs(tube1, 10)).isEmpty();

        clock.tick();
        List<TimeJobInfo> infos = scheduledJobSystem.removeExpiredReadyJobs(tube1, 10);
        infos.forEach(System.out::println);
        assertThat(infos).hasSize(3);


        // ready queues now empty
        assertThat(scheduledJobSystem.peekReady("A", tube1, 0, 1)).isEmpty();
        assertThat(scheduledJobSystem.peekReady("B", tube1, 0, 1)).isEmpty();
        assertThat(scheduledJobSystem.peekReady("C", tube1, 0, 1)).isEmpty();

        // assert subsequent reserve calls work (and prior expire calls cleaned state correctly)
        // note that we are making 3 separate calls because we can't currently reserve multiple jobs across channels
        clock.tick();
        assertThat(scheduledJobSystem.reserveMulti(tube1, 1000L, 1)).hasSize(1);
        assertThat(scheduledJobSystem.reserveMulti(tube1, 1000L, 1)).hasSize(1);
        assertThat(scheduledJobSystem.reserveMulti(tube1, 1000L, 1)).hasSize(1);

        assertThat(scheduleReader.getAllReadyJobCount(tube1)).isEqualTo(0);
        assertThat(scheduleReader.getRunningJobCount(tube1)).isEqualTo(3);
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

    @Test
    public void testGetAllReadyCount() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        String jobId = "doesnt-matter" + ":" + tube1;
        scheduledJobSystem.schedule("A", tube1, jobId + "_1", 0);
        scheduledJobSystem.schedule("B", tube1, jobId + "_2", 0);
        scheduledJobSystem.schedule("C", tube1, jobId + "_3", 0);

        assertThat(scheduleReader.getAllReadyJobCount(tube1)).isEqualTo(3);

    }

    @Test
    public void testReadyJobCountWithFutureReady() {

        TestClock clock = new TestClock(System.currentTimeMillis(), 10);
        MultiChannelScheduler scheduledJobSystem  = new MultiChannelScheduler(rdbi, prefix, clock);
        ScheduleReader scheduleReader = new ScheduleReader(rdbi, prefix, clock);

        // Schedule a job
        scheduledJobSystem.schedule("A", tube1, "{hello:world}", 20);

        // we should not count it as 'ready'
        assertThat(scheduleReader.getReadyJobCount("A", tube1)).isEqualTo(0);
        assertThat(scheduleReader.getAllReadyJobCount(tube1)).isEqualTo(0);

        clock.tick();
        // we should not count it as 'ready'
        assertThat(scheduleReader.getReadyJobCount("A", tube1)).isEqualTo(0);
        assertThat(scheduleReader.getAllReadyJobCount(tube1)).isEqualTo(0);

        clock.tick();
        // we should now count it as 'ready'
        assertThat(scheduleReader.getReadyJobCount("A", tube1)).isEqualTo(1);
        assertThat(scheduleReader.getAllReadyJobCount(tube1)).isEqualTo(1);
    }

    @Test
    public void testDeleteRunningWithoutReadyJob() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        scheduledJobSystem.enablePerChannelTracking();

        String jobId = "doesnt-matter" + ":" + tube1;
        scheduledJobSystem.schedule("A", tube1, jobId, 0);

        scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);

        assertThat(scheduledJobSystem.inRunningQueue(tube1, jobId)).isTrue();
        assertThat(scheduledJobSystem.inReadyQueue("A", tube1, jobId)).isFalse();

        assertThat(scheduledJobSystem.deleteJob("A", tube1, jobId)).isTrue();
        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(0);
    }

    @Test
    public void testDeleteJob() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        scheduledJobSystem.enablePerChannelTracking();

        String jobId = "doesnt-matter" + ":" + tube1;
        scheduledJobSystem.schedule("A", tube1, jobId, 0);

        scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);
        scheduledJobSystem.schedule("A", tube1, jobId, 0);

        // verify state - job in both ready + running
        assertThat(scheduledJobSystem.inRunningQueue(tube1, jobId)).isTrue();
        assertThat(scheduledJobSystem.inReadyQueue("A", tube1, jobId)).isTrue();

        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(1);
        assertThat(scheduledJobSystem.deleteJob("A", tube1, jobId)).isTrue();
        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(0);

        assertThat(scheduledJobSystem.deleteJob("A", tube1, jobId)).isFalse();
        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(0);


        // submit & reserve a job in another channel to make sure we cleaned up internal state
        scheduledJobSystem.schedule("B", tube1, jobId + "_1", 0);

        // re-verify state
        assertThat(scheduledJobSystem.inRunningQueue(tube1, jobId)).isFalse();
        assertThat(scheduledJobSystem.inReadyQueue("B", tube1, jobId)).isFalse();

        List<TimeJobInfo> reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);

        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_1");
        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(0);
        assertThat(scheduleReader.getRunningCountForChannel("B", tube1)).isEqualTo(1);

    }

    @Test
    public void testDeleteFromReady() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        String jobId = "doesnt-matter" + ":" + tube1;
        scheduledJobSystem.schedule("A", tube1, jobId, 0);

        // verify state - job in both ready
        assertThat(scheduledJobSystem.inReadyQueue("A", tube1, jobId)).isTrue();

        assertThat(scheduledJobSystem.deleteJobFromReady("A", tube1, jobId)).isTrue();

        // re-verify state
        assertThat(scheduledJobSystem.inRunningQueue(tube1, jobId)).isFalse();

        // submit & reserve a job in another channel to make sure we cleaned up internal state
        scheduledJobSystem.schedule("B", tube1, jobId + "_1", 0);
        assertThat(scheduledJobSystem.inReadyQueue("B", tube1, jobId)).isFalse();

        List<TimeJobInfo> reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);

        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_1");
    }

    @Test
    public void testKeepsLooking() {
        TestClock clock = new TestClock(System.currentTimeMillis(), 10);
        MultiChannelScheduler scheduledJobSystem  = new MultiChannelScheduler(rdbi, prefix, clock);
        String jobId = "doesnt-matter" + ":" + tube1;
        // Schedule some jobs in the future
        scheduledJobSystem.schedule("A", tube1, jobId + "_1", 10);
        scheduledJobSystem.schedule("B", tube1, jobId + "_2", 10);
        scheduledJobSystem.schedule("C", tube1, jobId + "_3", 10);

        // all in the future
        List<TimeJobInfo> reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);
        assertThat(reserved).isEmpty();

        // Schedule some jobs in the now
        scheduledJobSystem.schedule("A", tube1, jobId + "_1a", 0);
        scheduledJobSystem.schedule("B", tube1, jobId + "_2a", 0);
        scheduledJobSystem.schedule("C", tube1, jobId + "_3a", 0);


        // should get 3
        reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);
        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_1a");


        reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);
        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_2a");

        reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);
        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_3a");


        // all in the future
        reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);
        assertThat(reserved).isEmpty();


        clock.tick();

        reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);
        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_1");


        reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);
        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_2");

        reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 1);
        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_3");
    }

    // when reserving multi,
    // we can reserve across channels
    @Test
    public void testMultiReserveExpectations() {
        TestClock clock = new TestClock(System.currentTimeMillis(), 10);
        MultiChannelScheduler scheduledJobSystem  = new MultiChannelScheduler(rdbi, prefix, clock);
        String jobId = "doesnt-matter" + ":" + tube1;
        // Schedule some jobs in the future
        scheduledJobSystem.schedule("A", tube1, jobId + "_1", 0);
        scheduledJobSystem.schedule("B", tube1, jobId + "_2", 0);
        scheduledJobSystem.schedule("C", tube1, jobId + "_3", 0);

        // should get 3
        List<TimeJobInfo> reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 3);
        assertThat(reserved)
                .hasSize(3)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_1", jobId + "_2", jobId + "_3");
    }

    @Test
    public void testMultiReserveLimitations() {
        TestClock clock = new TestClock(System.currentTimeMillis(), 10);
        MultiChannelScheduler scheduledJobSystem  = new MultiChannelScheduler(rdbi, prefix, clock);
        String jobId = "doesnt-matter" + ":" + tube1;

        scheduledJobSystem.schedule("A", tube1, jobId + "_1", 0);
        scheduledJobSystem.schedule("B", tube1, jobId + "_2", 0);
        scheduledJobSystem.schedule("C", tube1, jobId + "_3", 0);

        clock.tick();
        scheduledJobSystem.schedule("A", tube1, jobId + "_1a", 0);
        scheduledJobSystem.schedule("A", tube1, jobId + "_1b", 0);


        // we might expect jobs from channel A,B,C, but that's now how it works now
        List<TimeJobInfo> reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 3);
        assertThat(reserved)
                .hasSize(3)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_1", jobId + "_1a", jobId + "_1b");

        reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 3);
        assertThat(reserved)
                .hasSize(2)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_2", jobId + "_3");
    }

    @Test
    public void testReserveWithRunningLimit() {
        MultiChannelScheduler scheduledJobSystem  = new MultiChannelScheduler(rdbi, prefix);
        String jobId = "doesnt-matter" + ":" + tube1;

        scheduledJobSystem.schedule("A", tube1, jobId + "_1", 0);
        scheduledJobSystem.schedule("B", tube1, jobId + "_2", 0);
        scheduledJobSystem.schedule("C", tube1, jobId + "_3", 0);


        List<TimeJobInfo> reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 2);
        assertThat(reserved)
                .hasSize(2)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_1", jobId + "_2");

        reserved = scheduledJobSystem.reserveMulti(tube1, 1_0000L, 2, 3);
        // tried to reserve 2 but 2 + 2 running would be > 3
        assertThat(reserved).isEmpty();

        reserved = scheduledJobSystem.reserveMulti(tube1, 1_0000L, 1, 2);
        // tried to reserve 1 but 2 + 1 running would be > 2
        assertThat(reserved).isEmpty();

        scheduledJobSystem.ackJob("A", tube1, jobId + "_1");
        reserved = scheduledJobSystem.reserveMulti(tube1, 1_0000L, 1, 2);
        // tried to reserve 1 but 1 + 1  <= 2 so we're good
        assertThat(reserved).hasSize(1)
                            .extracting(JobInfo::getJobStr)
                            .containsExactly(jobId + "_3");
    }


    @Test
    public void testReserveWithPerChannelAtScaleRunningLimit() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        scheduledJobSystem.enablePerChannelTracking();

        String jobId = "doesnt-matter" + ":" + tube1;


        IntStream.range(1, 3000).forEach(i -> {
            scheduledJobSystem.schedule("A", tube1, jobId + "_A" + i, 0);
        });

        // reserve 1
        scheduledJobSystem.reserveMulti(tube1, 1_000L, 1, 0, 1);

        System.out.println("Starting second reserve:");
        long start = System.currentTimeMillis();

        // attempt reserve another
        List<TimeJobInfo> reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 1, 0, 1);
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Finished reserve. elapsed = " + elapsed);

        assertThat(reserved).isEmpty();
        assertThat(elapsed).isLessThanOrEqualTo(10L);
    }

    @Test
    public void testReserveWithPerChannelRunningLimit() {
        MultiChannelScheduler scheduledJobSystem  = new MultiChannelScheduler(rdbi, prefix);
        scheduledJobSystem.enablePerChannelTracking();

        String jobId = "doesnt-matter" + ":" + tube1;

        scheduledJobSystem.schedule("A", tube1, jobId + "_A1", 0);
        scheduledJobSystem.schedule("B", tube1, jobId + "_B1", 0);
        scheduledJobSystem.schedule("C", tube1, jobId + "_C1", 0);

        scheduledJobSystem.schedule("A", tube1, jobId + "_A2", 0);
        scheduledJobSystem.schedule("A", tube1, jobId + "_A3", 0);
        scheduledJobSystem.schedule("A", tube1, jobId + "_A4", 0);

        List<TimeJobInfo> reserved = scheduledJobSystem.reserveMulti(tube1, 1_000L, 3, 0, 2);

        System.out.println(scheduleReader.getRunningCountForChannel("A", tube1));
        // we reserved 2 from a1 channel then hit our limit
        assertThat(reserved)
                .hasSize(3)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_A1", jobId + "_A2", jobId + "_B1");


        assertThat(scheduleReader.getRunningCountForChannel("A", tube1)).isEqualTo(2);

        reserved = scheduledJobSystem.reserveMulti(tube1, 1_0000L, 2, 0, 2);
        // tried to reserve 2 but, should get none for A but the one for C
        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_C1");

        reserved = scheduledJobSystem.reserveMulti(tube1, 1_0000L, 1, 0, 2);
        // tried to reserve 1 but still cannot
        assertThat(reserved).isEmpty();

        // if we add a job for another channel
        scheduledJobSystem.schedule("B", tube1, jobId + "_B2", 0);

        // we should get it but not the A job
        reserved = scheduledJobSystem.reserveMulti(tube1, 1_0000L, 2, 0, 2);
        // tried to reserve 2 but, should get 1 for channel A but not the other one
        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_B2");

        // now lets ack one
        scheduledJobSystem.ackJob("A", tube1, jobId + "_A1");
        reserved = scheduledJobSystem.reserveMulti(tube1, 1_0000L, 1, 0, 2);
        // we should be good
        assertThat(reserved).hasSize(1)
                            .extracting(JobInfo::getJobStr)
                            .containsExactly(jobId + "_A3");
    }

    @Test
    public void testReserveForChannel() {
        MultiChannelScheduler scheduledJobSystem = new MultiChannelScheduler(rdbi, prefix);
        String jobId = "doesnt-matter" + ":" + tube1;

        scheduledJobSystem.schedule("A", tube1, jobId + "_1", 0);
        scheduledJobSystem.schedule("B", tube1, jobId + "_2", 0);
        scheduledJobSystem.schedule("C", tube1, jobId + "_3", 0);


        // can reserve for B channel
        List<TimeJobInfo> reserved = scheduledJobSystem.reserveMultiForChannel("B", tube1, 1_000L, 1, 1);
        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_2");

        // can still reserve normally
        reserved = scheduledJobSystem.reserveMulti(tube1, 1_0000L, 1);
        assertThat(reserved)
                .hasSize(1)
                .extracting(JobInfo::getJobStr)
                .containsExactly(jobId + "_1");


        reserved = scheduledJobSystem.reserveMulti(tube1, 1_0000L, 1);
        assertThat(reserved).hasSize(1)
                            .extracting(JobInfo::getJobStr)
                            .containsExactly(jobId + "_3");

        // ensure that our "B" channel was removed from consideration
        List<String> mcItems = rdbi.open().jedis().lrange("mc-test::multichannel:tube1:circular_buffer", 0, -1);
        assertThat(mcItems).isEmpty();
    }
}
