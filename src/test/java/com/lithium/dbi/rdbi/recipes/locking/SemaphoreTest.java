package com.lithium.dbi.rdbi.recipes.locking;

import com.google.common.base.Optional;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(groups = "integration")
public class SemaphoreTest {
    private static final RDBI rdbi = new RDBI(new JedisPool("localhost"));
    private static final String ownerId = "ownerSemaphoreTest";
    private static final String lockKey = "lockKeySemaphoreTest";
    private static final Integer lockLengthInSeconds = 10;
    private Semaphore semaphore;

    @BeforeMethod
    public void beforeTest(){
        semaphore = new Semaphore(rdbi, ownerId, lockKey);
    }

    @AfterMethod
    public void afterTest(){
        semaphore.releaseLock();
    }

    @Test
    public void testAcquireRelease() {
        assertTrue(semaphore.acquireLock(lockLengthInSeconds));

        // Verify that we have deleted the lock
        Optional<String> realOwner = semaphore.releaseLock();
        assertTrue(realOwner.isPresent());
        assertEquals(ownerId, realOwner.get());
        try (Handle handle = rdbi.open()) {
            String checkOwnerId = handle.jedis().get(lockKey);
            assertNull(checkOwnerId);
        }
    }

    @Test
    public void testAcquireReleaseMultipleTimes() {
        assertTrue(semaphore.acquireLock(lockLengthInSeconds));

        // Verify cannot acquire a second time
        assertFalse(semaphore.acquireLock(lockLengthInSeconds));

        // Verify that we have deleted the lock
        Optional<String> realOwner = semaphore.releaseLock();
        assertTrue(realOwner.isPresent());
        assertEquals(ownerId, realOwner.get());
        try (Handle handle = rdbi.open()) {
            String checkOwnerId = handle.jedis().get(lockKey);
            assertNull(checkOwnerId);
        }

        // Verify that we cannot delete twice
        realOwner = semaphore.releaseLock();
        assertFalse(realOwner.isPresent());
    }

    @Test
    public void testAcquireReleaseDifferentOwner() {
        assertTrue(semaphore.acquireLock(lockLengthInSeconds));

        // Should not be able to acquire another owner's lock
        Semaphore semaphoreNew = new Semaphore(rdbi, ownerId + "Wrong", lockKey);
        assertFalse(semaphoreNew.acquireLock(lockLengthInSeconds));

        // Verify that cannot release another owner's lock
        Optional<String> realOwner = semaphoreNew.releaseLock();
        assertTrue(realOwner.isPresent());
        assertEquals(ownerId, realOwner.get());
        try (Handle handle = rdbi.open()) {
            String checkOwnerId = handle.jedis().get(lockKey);
            assertNotNull(checkOwnerId);
            assertEquals(ownerId, checkOwnerId);
        }

        // Verify that we have deleted the lock
        realOwner = semaphore.releaseLock();
        assertTrue(realOwner.isPresent());
        assertEquals(ownerId, realOwner.get());
    }
}
