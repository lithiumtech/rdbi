package com.lithium.dbi.rdbi.recipes.locking;

import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class MultiReadSingleWriteLock {

    private final RDBI rdbi;
    private final String writeLockKey;
    private final String readLockKey;
    private final int writeTimeoutMillis;
    private final int readTimeoutMillis;

    public MultiReadSingleWriteLock(RDBI rdbi,
                                    String writeLockKey,
                                    String readLockKey,
                                    int writeTimeoutMillis,
                                    int readTimeoutMillis) {
        this.rdbi = rdbi;
        this.writeLockKey = writeLockKey;
        this.readLockKey = readLockKey;
        this.writeTimeoutMillis = writeTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
    }

    public boolean acquireWriteLock(String ownerId) {
        try (Handle handle = rdbi.open()) {
            final MultiReadSingleWriteLockDAO dao = handle.attach(MultiReadSingleWriteLockDAO.class);
            while (true) {
                final int acquiredLock = dao.acquireWriteLock(writeLockKey, ownerId, writeTimeoutMillis);
                if (acquiredLock > 0) {
                    break;
                }

                Thread.sleep(250);
            }

            // If we wait for readers to quiesce before granting a write lock, a dead lock situation will occur for writers
            // when there is a constant never ending stream of new readers.
            //
            // To be fair to writers, we first grant the write lock (if available), in the loop above, so that any new readers
            // will temporarily be blocked until the new writer is done or times out.
            //
            // However, since there could be readers when we grant the write lock, we have to give the existing readers
            // a chance to relinquish their lock or time out before we unblock the new writer. So we'll wait in the loop below.
            while (true) {
                final Long readerCount = handle.jedis().zcount(readLockKey, Long.toString(Instant.now().getMillis()), "+inf");
                if (readerCount < 1) {
                    return true;
                }

                Thread.sleep(250);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // will never get here, we'll block until write lock is acquired and all readers have quiesced.
        return false;
    }

    public boolean releaseWriteLock(String ownerId) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(MultiReadSingleWriteLockDAO.class).releaseWriteLock(writeLockKey, ownerId) > 0;
        }
    }

    public boolean acquireReadLock(String ownerId) {
        try (Handle handle = rdbi.open()) {
            final MultiReadSingleWriteLockDAO dao = handle.attach(MultiReadSingleWriteLockDAO.class);
            while (true) {
                final int acquiredLock = dao.acquireReadLock(writeLockKey,
                                                             readLockKey,
                                                             ownerId,
                                                             Instant.now().plus(Duration.millis(readTimeoutMillis)).getMillis());
                if (acquiredLock > 0) {
                    return true;
                }

                Thread.sleep(250);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // will never get here, we'll block until write lock is acquired and all readers have quiesced.
        return false;
    }

    public boolean releaseReadLock(String ownerId) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(MultiReadSingleWriteLockDAO.class).releaseReadLock(readLockKey, Instant.now().getMillis(), ownerId) > 0;
        }
    }
}
