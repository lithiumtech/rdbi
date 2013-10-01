package com.lithium.dbi.rdbi.recipes.presence;

import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

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
        System.out.println("Time for 10,000 heartbeats " + Long.toString(after.getMillis() - before.getMillis()));

        assertTrue(after.getMillis() - before.getMillis() < 2000L);

        Instant before2 = Instant.now();
        for ( int i = 0; i < 10000; i++ ) {
            assertFalse(presenceRepository.expired("mytube", "id" + i));
        }
        Instant after2 = Instant.now();
        System.out.println("Time for 10,000 expired " + Long.toString(after2.getMillis() - before2.getMillis()));

        assertTrue(after2.getMillis() - before2.getMillis() < 2000L);

        Thread.sleep(10 * 1000L);

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
}
