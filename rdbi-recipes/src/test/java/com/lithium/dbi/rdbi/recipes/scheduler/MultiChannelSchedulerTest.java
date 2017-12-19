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
    private String channel2 = "channel2";
    private String channel3 = "channel3";

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
        // now we expect to get [ A, B, C, A, B, C, A, B, A, B, A, B, A, A, A, A, A ]


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


        final Consumer<String> reserveAndAssert = channel ->  {
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
        fail("not done");
    }

    @Test
    public void testRemoveExpiredReady() {
        fail("not done");
    }

    @Test
    public void testPause() {
        fail("not done");
    }

    @Test
    public void testPeekDelayed() {
        fail("not done");
    }

    @Test
    public void testPeekReady() {
        fail("not done");
    }

    @Test
    public void testPeekRunning() {
        fail("not done");
    }

    @Test
    public void testPeekExpired() {
        fail("not done");
    }

    @Test
    public void testInReadyQueue() {
        fail("not done");
    }



}
