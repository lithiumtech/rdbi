package com.lithium.dbi.rdbi.recipes.locking;

import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPool;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(groups = "integration")
public class RedisSemaphoreTest {
    private static final RDBI rdbi = new RDBI(new JedisPool("localhost" , 6379));
    private static final String ownerId = "ownerSemaphoreTest";
    private static final String semaphoreKey = "semaphoreKeySemaphoreTest";
    private static final Integer semaphoreTimeoutInSeconds = 1;
    private RedisSemaphore semaphore;

    @BeforeMethod
    public void beforeTest(){
        semaphore = new RedisSemaphore(rdbi, ownerId, semaphoreKey);
    }

    @AfterMethod
    public void afterTest(){
        semaphore.releaseSemaphore();
    }

    @Test
    public void testAcquireRelease() {
        assertTrue(semaphore.acquireSemaphore(semaphoreTimeoutInSeconds));

        // Verify that semaphore has been set
        try (Handle handle = rdbi.open()) {
            assertNotEquals(handle.jedis().setnx(semaphoreKey, ownerId), 1L);
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
    public void testAcquireTimeout() throws InterruptedException {
        assertTrue(semaphore.acquireSemaphore(semaphoreTimeoutInSeconds));
        Thread.sleep(semaphoreTimeoutInSeconds * 1000 + 100);

        // Verify that we have deleted the semaphore
        Optional<String> realOwner = semaphore.releaseSemaphore();
        assertFalse(realOwner.isPresent());
    }

    @Test
    public void testAcquireReleaseMultipleLocks() {
        assertTrue(semaphore.acquireSemaphore(semaphoreTimeoutInSeconds));

        // Acquire a second semaphore
        RedisSemaphore semaphoreDiffKey = new RedisSemaphore(rdbi, ownerId, semaphoreKey + "diffKey");
        assertTrue(semaphoreDiffKey.acquireSemaphore(semaphoreTimeoutInSeconds));

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
        assertTrue(semaphore.acquireSemaphore(semaphoreTimeoutInSeconds));

        // Verify cannot acquire a second time
        assertFalse(semaphore.acquireSemaphore(semaphoreTimeoutInSeconds));

        // Verify can reacquire
        assertTrue(semaphore.extendSemaphore(semaphoreTimeoutInSeconds));

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

        // Verify cannot reacquire if already released
        assertFalse(semaphore.extendSemaphore(semaphoreTimeoutInSeconds));
    }

    @Test
    public void testAcquireReleaseDifferentOwner() {
        assertTrue(semaphore.acquireSemaphore(semaphoreTimeoutInSeconds));

        // Verify can reacquire
        assertTrue(semaphore.extendSemaphore(semaphoreTimeoutInSeconds));

        // Should not be able to acquire another owner's semaphore
        RedisSemaphore semaphoreNew = new RedisSemaphore(rdbi, ownerId + "Wrong", semaphoreKey);
        assertFalse(semaphoreNew.acquireSemaphore(semaphoreTimeoutInSeconds));
        assertFalse(semaphoreNew.extendSemaphore(semaphoreTimeoutInSeconds));

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
