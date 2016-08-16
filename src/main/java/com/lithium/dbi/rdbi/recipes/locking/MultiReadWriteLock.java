package com.lithium.dbi.rdbi.recipes.locking;

import com.google.common.base.Throwables;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class MultiReadWriteLock {

    private final RDBI rdbi;
    private final String writeLockKey;
    private final String readLockKey;
    private final int writeTimeoutMillis;
    private final int readTimeoutMillis;

    public MultiReadWriteLock(RDBI rdbi,
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
            final MultiReadWriteLockDAO dao = handle.attach(MultiReadWriteLockDAO.class);
            while (true) {
                final int acquiredLock = dao.acquireWriteLock(writeLockKey, ownerId, writeTimeoutMillis);
                if (acquiredLock > 0) {
                    break;
                }

                Thread.sleep(250);
            }

            while (true) {
                final Long readerCount = handle.jedis().zcount(readLockKey, Long.toString(Instant.now().getMillis()), "+inf");
                if (readerCount < 1) {
                    return true;
                }

                Thread.sleep(250);
            }
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }

        // will never get here, we'll block until write lock is acquired
        return false;
    }

    public boolean releaseWriteLock(String ownerId) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(MultiReadWriteLockDAO.class).releaseWriteLock(writeLockKey, ownerId) > 0;
        }
    }

    public boolean acquireReadLock(String ownerId) {
        try (Handle handle = rdbi.open()) {
            final MultiReadWriteLockDAO dao = handle.attach(MultiReadWriteLockDAO.class);
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
            Throwables.propagate(e);
        }

        // will never get here, we'll block until read lock is acquired
        return false;
    }

    public boolean releaseReadLock(String ownerId) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(MultiReadWriteLockDAO.class).releaseReadLock(readLockKey, Instant.now().getMillis(), ownerId) > 0;
        }
    }
}
