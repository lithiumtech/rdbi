package com.lithium.dbi.rdbi.recipes.presence;

import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.lithium.dbi.rdbi.testutil.Utils.assertTiming;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "integration")
public class PresenceRepositoryTest {

    @Test
    public void addTest() throws InterruptedException {

        final PresenceRepository presenceRepository = new PresenceRepository(new RDBI(new JedisPool("localhost", 6379)), "myprefix");

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

        final PresenceRepository presenceRepository = new PresenceRepository(new RDBI(new JedisPool("localhost", 6379)), "myprefix");
        presenceRepository.nukeForTest("mytube");

        assertTiming(2000, TimeUnit.MILLISECONDS,
                     () -> IntStream
                             .range(0, 10_000)
                             .forEach(i -> presenceRepository.addHeartbeat("mytube", "id" + i, 10 * 1000L))
                    );

        assertTiming(2000, TimeUnit.MILLISECONDS,
                     () -> IntStream
                             .range(0, 10_000)
                             .forEach(i -> assertFalse(presenceRepository.expired("mytube", "id" + i)))
                    );
        Thread.sleep(2_000L);

        assertTiming(2000, TimeUnit.MILLISECONDS,
                     () -> IntStream
                             .range(0, 5_000)
                             .forEach(i -> assertTrue(presenceRepository.remove("mytube", "id" + i)))
                    );

        assertTiming(500L, TimeUnit.MILLISECONDS,
                     () ->  presenceRepository.cull("mytube")
                    );

    }

    @Test
    public void getPresentTest() throws InterruptedException {
        final String mytube = "getPresentTest";
        final PresenceRepository presenceRepository = new PresenceRepository(new RDBI(new JedisPool("localhost", 6379)), "myprefix");
        presenceRepository.nukeForTest(mytube);

        // assert set is empty at start
        assertTrue(presenceRepository.getPresent(mytube, Optional.empty()).isEmpty());

        // put something in and verify we can get it back out
        final String uuid = UUID.randomUUID().toString();
        presenceRepository.addHeartbeat(mytube, uuid, Duration.ofSeconds(1).toMillis());
        final List<String> presentSet = presenceRepository.getPresent(mytube, Optional.empty());
        assertEquals(uuid, presentSet.iterator().next(), "Expected to have one heartbeat with uuid: " + uuid);

        // call cull and verify heart beat is still present
        presenceRepository.cull(mytube);
        final List<String> stillpresentSet = presenceRepository.getPresent(mytube, Optional.empty());
        assertEquals(stillpresentSet.iterator().next(), uuid, "Expected to still have one heartbeat with uuid: " + uuid);

        // wait a second and verify previous heartbeat is expired
        final Instant beforeSleep = Instant.now();
        while (true) {
            Thread.sleep(Duration.ofSeconds(1).toMillis());
            if (Duration.between(beforeSleep, Instant.now()).compareTo(Duration.ofSeconds(1)) > 0) {
                break;
            }
        }
        List<String> tubeContents = presenceRepository.getPresent(mytube, Optional.empty());
        assertTrue(tubeContents.isEmpty(), String.format("tube contents should be empty, but are %s", tubeContents));

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
        final PresenceRepository presenceRepository = new PresenceRepository(new RDBI(new JedisPool("localhost", 6379)), "myprefix");
        presenceRepository.nukeForTest(mytube);

        // assert set is empty at start
        assertTrue(presenceRepository.getPresent(mytube, Optional.empty()).isEmpty());

        // put something in and verify we can get it back out
        final String uuid = UUID.randomUUID().toString();
        presenceRepository.addHeartbeat(mytube, uuid, Duration.ofSeconds(1).toMillis());
        final List<String> presentSet = presenceRepository.getPresent(mytube, Optional.empty());
        assertEquals(uuid, presentSet.iterator().next(), "Expected to have one heartbeat with uuid: " + uuid);

        // call cull and verify heart beat is still present
        presenceRepository.cull(mytube);
        final List<String> stillpresentSet = presenceRepository.getPresent(mytube, Optional.empty());
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
