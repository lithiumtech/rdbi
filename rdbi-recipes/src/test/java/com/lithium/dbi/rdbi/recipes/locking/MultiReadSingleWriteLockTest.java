package com.lithium.dbi.rdbi.recipes.locking;

import com.google.common.collect.ImmutableList;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "integration")
public class MultiReadSingleWriteLockTest {
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), REDIS_HOST, REDIS_PORT, Protocol.DEFAULT_TIMEOUT);
    private final RDBI rdbi = new RDBI(jedisPool);

    private String writeLockKey;
    private String readLockKey;

    @BeforeMethod
    public void setupTests() {
        final String lockKey = UUID.randomUUID().toString();
        writeLockKey = String.format("%s:multiReadWriteLockTest:write", lockKey);
        readLockKey = String.format("%s:multiReadWriteLockTest:read", lockKey);
    }

    @Test
    public void testAcquireWriteLock_simpleAcquireAndRelease() throws Exception {
        final MultiReadSingleWriteLock lock = new MultiReadSingleWriteLock(rdbi,
                                                                           writeLockKey,
                                                                           readLockKey,
                                                                           (int) TimeUnit.MINUTES.toMillis(1),
                                                                           (int) TimeUnit.MINUTES.toMillis(1));

        final String lockOwnerId = UUID.randomUUID().toString();
        try (Handle handle = rdbi.open()) {
            // start by checking that no one owns lock
            assertNull(handle.jedis().get(writeLockKey));

            // acquire lock and verify we own the lock
            lock.acquireWriteLock(lockOwnerId);
            assertEquals(lockOwnerId, handle.jedis().get(writeLockKey));

            // release lock and verify that we no longer own the lock
            lock.releaseWriteLock(lockOwnerId);
            assertNull(handle.jedis().get(writeLockKey));
        }
    }

    @Test
    public void testAcquireWriteLock_writeLockExpiration() throws Exception {
        final MultiReadSingleWriteLock lock = new MultiReadSingleWriteLock(rdbi,
                                                                           writeLockKey,
                                                                           readLockKey,
                                                                           500,
                                                                           500);
        final String lockOwnerId = UUID.randomUUID().toString();
        try (Handle handle = rdbi.open()) {
            // start by checking that no one owns lock
            assertNull(handle.jedis().get(writeLockKey));

            // acquire lock and verify we own the lock...
            // but we're not going to release lock because our thread died or whatever
            lock.acquireWriteLock(lockOwnerId);
            assertEquals(lockOwnerId, handle.jedis().get(writeLockKey));

            // someone else waiting for lock will eventually get it
            final String newLockOwnerId = UUID.randomUUID().toString();
            lock.acquireWriteLock(newLockOwnerId);
            assertEquals(newLockOwnerId, handle.jedis().get(writeLockKey));

//            // wait for new owner to expire and check that no one owns lock
//            final Instant beyondExpiration = Instant.now().plus(Duration.ofMillis(500));
//            while (true) {
//                Thread.sleep(100);
//                if (Instant.now().isAfter(beyondExpiration)) {
//                    break;
//                }
//            }
            await()
                    .atLeast(Duration.ofMillis(500))
                    .atMost(Duration.ofSeconds(1))
                    .untilAsserted(() -> assertNull(handle.jedis().get(writeLockKey)));
        }
    }

    @Test
    public void testAcquireReadLock_simpleAcquireAndRelease() throws Exception {
        final MultiReadSingleWriteLock lock = new MultiReadSingleWriteLock(rdbi,
                                                                           writeLockKey,
                                                                           readLockKey,
                                                                           (int) TimeUnit.MINUTES.toMillis(1),
                                                                           (int) TimeUnit.MINUTES.toMillis(1));

        final String lockOwnerId = UUID.randomUUID().toString();
        try (Handle handle = rdbi.open()) {
            // start by checking that no one has any read locks
            assertTrue(handle.jedis().zrangeByScore(readLockKey, Long.toString(Instant.now().toEpochMilli()), "+inf", 0, 1).isEmpty());

            // acquire a read lock and verify that we own a read lock
            lock.acquireReadLock(lockOwnerId);
            final List<String> owners = handle.jedis().zrangeByScore(readLockKey, Long.toString(Instant.now().toEpochMilli()), "+inf");
            assertEquals(1, owners.size());
            assertTrue(owners.contains(lockOwnerId));

            // release the lock and verify that no one owns a ready lock
            lock.releaseReadLock(lockOwnerId);
            assertTrue(handle.jedis().zrangeByScore(readLockKey, Long.toString(Instant.now().toEpochMilli()), "+inf", 0, 1).isEmpty());

            // acquire read locks for 3 different owners and verify
            final String newOwner1 = UUID.randomUUID().toString();
            final String newOwner2 = UUID.randomUUID().toString();
            final String newOwner3 = UUID.randomUUID().toString();
            lock.acquireReadLock(newOwner1);
            lock.acquireReadLock(newOwner2);
            lock.acquireReadLock(newOwner3);
            final List<String> newOwners = handle.jedis().zrangeByScore(readLockKey, Long.toString(Instant.now().toEpochMilli()), "+inf");
            assertEquals(3, newOwners.size());
            assertTrue(newOwners.containsAll(ImmutableList.of(newOwner1, newOwner2, newOwner3)));

            // release one lock and verify the others are still there
            lock.releaseReadLock(newOwner1);
            final List<String> newOwner2And3 = handle.jedis().zrangeByScore(readLockKey, Long.toString(Instant.now().toEpochMilli()), "+inf");
            assertEquals(2, newOwner2And3.size());
            assertTrue(newOwner2And3.containsAll(ImmutableList.of(newOwner2, newOwner3)));

            // release everything
            lock.releaseReadLock(newOwner2);
            lock.releaseReadLock(newOwner3);
            assertTrue(handle.jedis().zrangeByScore(readLockKey, Long.toString(Instant.now().toEpochMilli()), "+inf", 0, 1).isEmpty());
        }
    }

    @Test
    public void testAcquireReadLock_readLockExpiration() throws Exception {
        final MultiReadSingleWriteLock lock = new MultiReadSingleWriteLock(rdbi,
                                                                           writeLockKey,
                                                                           readLockKey,
                                                                           500,
                                                                           500);

        final String lockOwnerId = UUID.randomUUID().toString();
        try (Handle handle = rdbi.open()) {
            // start by checking that no one has any read locks
            assertTrue(handle.jedis().zrangeByScore(readLockKey, Long.toString(Instant.now().toEpochMilli()), "+inf", 0, 1).isEmpty());

            // acquire a read lock but let's not release it...
            lock.acquireReadLock(lockOwnerId);
            final List<String> owners = handle.jedis().zrangeByScore(readLockKey, Long.toString(Instant.now().toEpochMilli()), "+inf");
            assertEquals(1, owners.size());
            assertTrue(owners.contains(lockOwnerId));

            // wait for expiration and verify the lock has expired
            final Instant beyondExpiration = Instant.now().plus(Duration.ofMillis(500));
            while (true) {
                Thread.sleep(100);
                if (Instant.now().isAfter(beyondExpiration)) {
                    break;
                }
            }
            assertTrue(handle.jedis().zrangeByScore(readLockKey, Long.toString(Instant.now().toEpochMilli()), "+inf", 0, 1).isEmpty());
        }
    }

    @Test
    public void testAcquireWriteLock_blockedByReads() throws Exception {
        final MultiReadSingleWriteLock lock = new MultiReadSingleWriteLock(rdbi,
                                                                           writeLockKey,
                                                                           readLockKey,
                                                                           500,
                                                                           500);

        final String readLockOwner = UUID.randomUUID().toString();
        final String writeLockOwner = UUID.randomUUID().toString();
        // acquire a read lock, this should prevent all write lock acquisitions until expiration
        final Instant expiration = Instant.now().plus(Duration.ofMillis(500));
        lock.acquireReadLock(readLockOwner);

        // acquire a write lock, this should block until read lock is expired
        lock.acquireWriteLock(writeLockOwner);
        assertTrue(Instant.now().isAfter(expiration));
    }

    @Test
    public void testAcquireReadLock_blockedByWrite() throws Exception {
        final MultiReadSingleWriteLock lock = new MultiReadSingleWriteLock(rdbi,
                                                                           writeLockKey,
                                                                           readLockKey,
                                                                           500,
                                                                           500);

        final String readLockOwner = UUID.randomUUID().toString();
        final String writeLockOwner = UUID.randomUUID().toString();

        // acquire a write lock, this should prevent all read lock acquisitions until expiration
        final Instant expiration = Instant.now().plus(Duration.ofMillis(500));
        lock.acquireWriteLock(writeLockOwner);

        // acquire a read lock, this should block until write lock is expired
        lock.acquireReadLock(readLockOwner);
        assertTrue(Instant.now().isAfter(expiration));
    }

    @Test(timeOut = 5000L)
    public void testReacquireReadLock() throws Exception {
        final MultiReadSingleWriteLock lock = new MultiReadSingleWriteLock(rdbi,
                                                                           writeLockKey,
                                                                           readLockKey,
                                                                           500,
                                                                           500);

        final String readLockOwner = UUID.randomUUID().toString();

        // acquire a read lock, this should block until write lock is expired
        assertTrue(lock.acquireReadLock(readLockOwner));
        assertTrue(lock.acquireReadLock(readLockOwner));
    }
}
