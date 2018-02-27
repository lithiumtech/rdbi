package com.lithium.dbi.rdbi.recipes.presence;

import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
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
            presenceRepository.addHeartbeat("mytube", "id" + i, 10 * 1000L);
        }
        Instant after = Instant.now();
        System.out.println("Time for 10,000 heartbeats " + Long.toString(after.toEpochMilli() - before.toEpochMilli()));

        assertTrue(after.toEpochMilli() - before.toEpochMilli() < 2000L);

        Instant before2 = Instant.now();
        for ( int i = 0; i < 10000; i++ ) {
            assertFalse(presenceRepository.expired("mytube", "id" + i));
        }
        Instant after2 = Instant.now();
        System.out.println("Time for 10,000 expired " + Long.toString(after2.toEpochMilli() - before2.toEpochMilli()));

        assertTrue(after2.toEpochMilli() - before2.toEpochMilli() < 2000L);

        Thread.sleep(10 * 1000L);

        Instant before3 = Instant.now();
        for ( int i = 0; i < 5000; i++ ) {
            assertTrue(presenceRepository.remove("mytube", "id" + i));
        }
        Instant after3 = Instant.now();
        System.out.println("Time for 5000 removes " + Long.toString(after3.toEpochMilli() - before3.toEpochMilli()));

        assertTrue(after3.toEpochMilli() - before3.toEpochMilli() < 1000L);

        Instant before4 = Instant.now();
        presenceRepository.cull("mytube");
        Instant after4 = Instant.now();
        System.out.println("Time for 5000 cull " + Long.toString(after4.toEpochMilli() - before4.toEpochMilli()));

        assertTrue(after4.toEpochMilli() - before4.toEpochMilli() < 500L);
    }

    @Test
    public void getPresentTest() throws InterruptedException {
        final String mytube = "getPresentTest";
        final PresenceRepository presenceRepository = new PresenceRepository(new RDBI(new JedisPool("localhost")), "myprefix");
        presenceRepository.nukeForTest(mytube);

        // assert set is empty at start
        assertTrue(presenceRepository.getPresent(mytube, Optional.empty()).isEmpty());

        // put something in and verify we can get it back out
        final String uuid = UUID.randomUUID().toString();
        presenceRepository.addHeartbeat(mytube, uuid, Duration.ofSeconds(1).toMillis());
        final Set<String> presentSet = presenceRepository.getPresent(mytube, Optional.empty());
        assertEquals(uuid, presentSet.iterator().next(), "Expected to have one heartbeat with uuid: " + uuid);

        // call cull and verify heart beat is still present
        presenceRepository.cull(mytube);
        final Set<String> stillpresentSet = presenceRepository.getPresent(mytube, Optional.empty());
        assertEquals(stillpresentSet.iterator().next(), uuid, "Expected to still have one heartbeat with uuid: " + uuid);

        // wait a second and verify previous heartbeat is expired
        final Instant beforeSleep = Instant.now();
        while (true) {
            Thread.sleep(Duration.ofSeconds(1).toMillis());
            if (Duration.between(beforeSleep, Instant.now()).compareTo(Duration.ofSeconds(1)) > 0) {
                break;
            }
        }
        assertTrue(presenceRepository.getPresent(mytube, Optional.empty()).isEmpty());

        // test with limit will not return full set
        for (int i = 0; i < 100; ++i) {
            presenceRepository.addHeartbeat(mytube, UUID.randomUUID().toString(), Duration.ofMinutes(1).toMillis());
        }
        assertEquals(100, presenceRepository.getPresent(mytube, Optional.empty()).size());
        assertEquals(10, presenceRepository.getPresent(mytube, Optional.of(10)).size());
    }

    @Test
    public void getExpiredTest() throws InterruptedException {
        final String mytube = "getPresentTest";
        final PresenceRepository presenceRepository = new PresenceRepository(new RDBI(new JedisPool("localhost")), "myprefix");
        presenceRepository.nukeForTest(mytube);

        // assert set is empty at start
        assertTrue(presenceRepository.getPresent(mytube, Optional.empty()).isEmpty());

        // put something in and verify we can get it back out
        final String uuid = UUID.randomUUID().toString();
        presenceRepository.addHeartbeat(mytube, uuid, Duration.ofSeconds(1).toMillis());
        final Set<String> presentSet = presenceRepository.getPresent(mytube, Optional.empty());
        assertEquals(uuid, presentSet.iterator().next(), "Expected to have one heartbeat with uuid: " + uuid);

        // call cull and verify heart beat is still present
        presenceRepository.cull(mytube);
        final Set<String> stillpresentSet = presenceRepository.getPresent(mytube, Optional.empty());
        assertEquals(stillpresentSet.iterator().next(), uuid, "Expected to still have one heartbeat with uuid: " + uuid);

        // wait a second and verify previous heartbeat is expired
        final Instant beforeSleep = Instant.now();
        while (true) {
            Thread.sleep(Duration.ofSeconds(1).toMillis());
            if (Duration.between(beforeSleep, Instant.now()).compareTo(Duration.ofSeconds(1)) > 0) {
                break;
            }
        }
        assertFalse(presenceRepository.getExpired(mytube, Optional.empty()).isEmpty());

        // test with limit will not return full set
        for (int i = 0; i < 100; ++i) {
            presenceRepository.addHeartbeat(mytube, UUID.randomUUID().toString(), Duration.ofMinutes(1).toMillis());
        }
        assertEquals(100, presenceRepository.getPresent(mytube, Optional.empty()).size());
        assertEquals(10, presenceRepository.getPresent(mytube, Optional.of(10)).size());
    }
}
