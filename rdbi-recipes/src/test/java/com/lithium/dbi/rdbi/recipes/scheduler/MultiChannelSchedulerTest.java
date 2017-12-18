package com.lithium.dbi.rdbi.recipes.scheduler;

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
import static org.testng.Assert.assertTrue;

@Test(groups = "integration")
public class MultiChannelSchedulerTest {
    private static final RDBI rdbi = new RDBI(new JedisPool("localhost"));
    private String tubeName;

    private MultiChannelScheduler scheduledJobSystem = null;

    @BeforeMethod
    public void setup(){
        tubeName = TubeUtils.uniqueTubeName();
        scheduledJobSystem  = new MultiChannelScheduler(rdbi, "myprefix:");
    }

    @AfterMethod
    public void tearDown(){
        try (Handle handle = rdbi.open()) {
            //TODO we should actually have the full list of touched things (even if they've cleared out)
            handle.jedis().del("myprefix::multichannel:mygroup1:set",
                    "myprefix::tube:mygroup1:tube1:running_queue",
                    "myprefix::multichannel:mygroup1:circular_buffer",
                    "myprefix::tube:mygroup1:tube2:running_queue"
                    );
        }
    }

    @Test
    public void basicTest() throws InterruptedException {
        boolean schedule1 = scheduledJobSystem.schedule("mygroup1", "tube1", "mygroup1:mytube1:myjob1", 0);
        boolean schedule2 = scheduledJobSystem.schedule("mygroup1", "tube2", "mygroup1:mytube2:myjob1", 0);
        boolean schedule3 = scheduledJobSystem.schedule("mygroup1", "tube3", "mygroup1:mytube3:myjob1", 0);

        List<String> running = scheduledJobSystem.getAllChannels("mygroup1");
        List<TimeJobInfo> job1 = scheduledJobSystem.reserveMulti("mygroup1", 1000, 1);
        List<TimeJobInfo> job2 = scheduledJobSystem.reserveMulti("mygroup1", 1000, 1);
        List<TimeJobInfo> job3 = scheduledJobSystem.reserveMulti("mygroup1", 1000, 1);

        List<TimeJobInfo> job4 =  scheduledJobSystem.reserveMulti("mygroup1", 1000, 1);
        List<TimeJobInfo> job5 =  scheduledJobSystem.reserveMulti("mygroup1", 1000, 1);

        boolean ack1 = scheduledJobSystem.ackJob("mygroup1", "tube1", job1.get(0).getJobStr());
        boolean ack2 = scheduledJobSystem.ackJob("mygroup1", "tube2", job2.get(0).getJobStr());

        Thread.sleep(1_001);
        List<TimeJobInfo> expiredJobs = scheduledJobSystem.removeExpiredRunningJobs("mygroup1", "tube3");

        assertTrue(schedule1);
        assertTrue(schedule2);
        assertTrue(schedule3);

        assertEquals(running.size(), 3);
        assertEquals(job1.get(0).getJobStr(), "mygroup1:mytube1:myjob1");
        assertEquals(job2.get(0).getJobStr(), "mygroup1:mytube2:myjob1");
        assertEquals(job3.get(0).getJobStr(), "mygroup1:mytube3:myjob1");
        assertTrue(job4.isEmpty());
        assertTrue(job5.isEmpty());

        assertTrue(ack1);
        assertTrue(ack2);

        assertEquals(expiredJobs.get(0).getJobStr(), "mygroup1:mytube3:myjob1");
    }
}
