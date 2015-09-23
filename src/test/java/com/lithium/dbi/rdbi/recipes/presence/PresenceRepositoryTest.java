package com.lithium.dbi.rdbi.recipes.presence;

import com.google.common.base.Optional;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Minutes;
import org.joda.time.Seconds;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "integration")
public class PresenceRepositoryTest {

    @Test
    public void addTest () throws InterruptedException {

        final PresenceRepository presenceRepository = new PresenceRepository(new RDBI(new JedisPool("localhost")), "myprefix");

        presenceRepository.nukeForTest("mytube");

        assertTrue(presenceRepository.expired("mytube", "id1"));
        presenceRepository.addHeartbeat("mytube", "id1", 1000L);

        assertFalse(presenceRepository.expired("mytube", "id1"));
        presenceRepository.cull("mytube");
        assertFalse(presenceRepository.expired("mytube", "id1"));

        Thread.sleep(1500L);
        assertTrue(presenceRepository.expired("mytube", "id1"));
        presenceRepository.cull("mytube");
        assertTrue(presenceRepository.expired("mytube", "id1"));
    }

    @Test
    public void basicPerformanceTest() throws InterruptedException {

        final PresenceRepository presenceRepository = new PresenceRepository(new RDBI(new JedisPool("localhost")), "myprefix");
        presenceRepository.nukeForTest("mytube");

        Instant before = Instant.now();
        for ( int i = 0; i < 10000; i++ ) {
            presenceRepository.addHeartbeat("mytube", "id" + i, Duration.standardSeconds(20).getMillis());
        }
        Instant after = Instant.now();
        System.out.println("Time for 10,000 heartbeats " + Long.toString(after.getMillis() - before.getMillis()));

        assertTrue(after.getMillis() - before.getMillis() < 2000L);

        Instant before2 = Instant.now();
        for ( int i = 0; i < 10000; i++ ) {
            assertFalse(presenceRepository.expired("mytube", "id" + i));
        }
        Instant after2 = Instant.now();
        System.out.println("Time for 10,000 expired " + Long.toString(after2.getMillis() - before2.getMillis()));

        assertTrue(after2.getMillis() - before2.getMillis() < 2000L);

        Instant before3 = Instant.now();
        for ( int i = 0; i < 5000; i++ ) {
            assertTrue(presenceRepository.remove("mytube", "id" + i));
        }
        Instant after3 = Instant.now();
        System.out.println("Time for 5000 removes " + Long.toString(after3.getMillis() - before3.getMillis()));

        assertTrue(after3.getMillis() - before3.getMillis() < 1000L);

        Instant before4 = Instant.now();
        presenceRepository.cull("mytube");
        Instant after4 = Instant.now();
        System.out.println("Time for 5000 cull " + Long.toString(after4.getMillis() - before4.getMillis()));

        assertTrue(after4.getMillis() - before4.getMillis() < 500L);
    }

    @Test
    public void getPresentTest() throws InterruptedException {
        final String mytube = "getPresentTest";
        final PresenceRepository presenceRepository = new PresenceRepository(new RDBI(new JedisPool("localhost")), "myprefix");
        presenceRepository.nukeForTest(mytube);

        // assert set is empty at start
        assertTrue(presenceRepository.getPresent(mytube, Optional.<Integer>absent()).isEmpty());

        // put something in and verify we can get it back out
        final String uuid = UUID.randomUUID().toString();
        presenceRepository.addHeartbeat(mytube, uuid, Seconds.seconds(1).toStandardDuration().getMillis());
        final Set<String> presentSet = presenceRepository.getPresent(mytube, Optional.<Integer>absent());
        assertEquals(uuid, presentSet.iterator().next(), "Expected to have one heartbeat with uuid: " + uuid);

        // call cull and verify heart beat is still present
        presenceRepository.cull(mytube);
        final Set<String> stillpresentSet = presenceRepository.getPresent(mytube, Optional.<Integer>absent());
        assertEquals(stillpresentSet.iterator().next(), uuid, "Expected to still have one heartbeat with uuid: " + uuid);

        // wait a second and verify previous heartbeat is expired
        final Instant beforeSleep = Instant.now();
        while (true) {
            Thread.sleep(Duration.standardSeconds(1).getMillis());
            if (new Duration(beforeSleep, Instant.now()).isLongerThan(Duration.standardSeconds(1))) {
                break;
            }
        }
        assertTrue(presenceRepository.getPresent(mytube, Optional.<Integer>absent()).isEmpty());

        // test with limit will not return full set
        for (int i = 0; i < 100; ++i) {
            presenceRepository.addHeartbeat(mytube, UUID.randomUUID().toString(), Minutes.ONE.toStandardDuration().getMillis());
        }
        assertEquals(100, presenceRepository.getPresent(mytube, Optional.<Integer>absent()).size());
        assertEquals(10, presenceRepository.getPresent(mytube, Optional.of(10)).size());
    }
}
