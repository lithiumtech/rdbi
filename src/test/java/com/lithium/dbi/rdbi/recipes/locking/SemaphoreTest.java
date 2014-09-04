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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(groups = "integration")
public class SemaphoreTest {
    private static final RDBI rdbi = new RDBI(new JedisPool("localhost"));
    private static final String ownerId = "ownerSemaphoreTest";
    private static final String semaphoreKey = "semaphoreKeySemaphoreTest";
    private static final Integer semaphoreLengthInSeconds = 10;
    private Semaphore semaphore;

    @BeforeMethod
    public void beforeTest(){
        semaphore = new Semaphore(rdbi, ownerId, semaphoreKey);
    }

    @AfterMethod
    public void afterTest(){
        semaphore.releaseSemaphore();
    }

    @Test
    public void testAcquireRelease() {
        assertTrue(semaphore.acquireSemaphore(semaphoreLengthInSeconds));
        try (Handle handle = rdbi.open()) {
            assertNotEquals(handle.jedis().setnx(semaphoreKey, ownerId + "New"), 1L);
        }

        // Verify that we have deleted the semaphore
        Optional<String> realOwner = semaphore.releaseSemaphore();
        assertTrue(realOwner.isPresent());
        assertEquals(ownerId, realOwner.get());
        try (Handle handle = rdbi.open()) {
            String checkOwnerId = handle.jedis().get(semaphoreKey);
            assertNull(checkOwnerId);
        }
    }

    @Test
    public void testAcquireReleaseMultipleLocks() {
        assertTrue(semaphore.acquireSemaphore(semaphoreLengthInSeconds));

        // Acquire a second semaphore
        Semaphore semaphoreDiffKey = new Semaphore(rdbi, ownerId, semaphoreKey + "diffKey");
        assertTrue(semaphoreDiffKey.acquireSemaphore(semaphoreLengthInSeconds));

        // Verify that we have deleted the semaphore
        Optional<String> realOwner = semaphore.releaseSemaphore();
        assertTrue(realOwner.isPresent());
        assertEquals(ownerId, realOwner.get());
        try (Handle handle = rdbi.open()) {
            String checkOwnerId = handle.jedis().get(semaphoreKey);
            assertNull(checkOwnerId);

            checkOwnerId = handle.jedis().get(semaphoreKey + "diffKey");
            assertNotNull(checkOwnerId);
        }

        // Verify that we have deleted the semaphore
        realOwner = semaphoreDiffKey.releaseSemaphore();
        assertTrue(realOwner.isPresent());
        assertEquals(ownerId, realOwner.get());
    }

    @Test
    public void testAcquireReleaseMultipleTimes() {
        assertTrue(semaphore.acquireSemaphore(semaphoreLengthInSeconds));

        // Verify cannot acquire a second time
        assertFalse(semaphore.acquireSemaphore(semaphoreLengthInSeconds));

        // Verify that we have deleted the semaphore
        Optional<String> realOwner = semaphore.releaseSemaphore();
        assertTrue(realOwner.isPresent());
        assertEquals(ownerId, realOwner.get());
        try (Handle handle = rdbi.open()) {
            String checkOwnerId = handle.jedis().get(semaphoreKey);
            assertNull(checkOwnerId);
        }

        // Verify that we cannot delete twice
        realOwner = semaphore.releaseSemaphore();
        assertFalse(realOwner.isPresent());
    }

    @Test
    public void testAcquireReleaseDifferentOwner() {
        assertTrue(semaphore.acquireSemaphore(semaphoreLengthInSeconds));

        // Should not be able to acquire another owner's semaphore
        Semaphore semaphoreNew = new Semaphore(rdbi, ownerId + "Wrong", semaphoreKey);
        assertFalse(semaphoreNew.acquireSemaphore(semaphoreLengthInSeconds));

        // Verify that cannot release another owner's semaphore
        Optional<String> realOwner = semaphoreNew.releaseSemaphore();
        assertTrue(realOwner.isPresent());
        assertEquals(ownerId, realOwner.get());
        try (Handle handle = rdbi.open()) {
            String checkOwnerId = handle.jedis().get(semaphoreKey);
            assertNotNull(checkOwnerId);
            assertEquals(ownerId, checkOwnerId);
        }

        // Verify that we have deleted the semaphore
        realOwner = semaphore.releaseSemaphore();
        assertTrue(realOwner.isPresent());
        assertEquals(ownerId, realOwner.get());
    }
}
