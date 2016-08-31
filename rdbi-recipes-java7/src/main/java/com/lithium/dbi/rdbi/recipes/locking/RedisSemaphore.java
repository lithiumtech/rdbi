package com.lithium.dbi.rdbi.recipes.locking;

import com.google.common.base.Optional;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;

public class RedisSemaphore {
    private final RDBI rdbi;
    private final String ownerId;
    private final String semaphoreKey;

    public RedisSemaphore(RDBI rdbi, String ownerId, String semaphoreKey) {
        this.rdbi = rdbi;
        this.ownerId = ownerId;
        this.semaphoreKey = semaphoreKey;
    }

    public boolean acquireSemaphore(final Integer semaphoreExpireSeconds) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(RedisSemaphoreDAO.class)
                              .acquireSemaphore(semaphoreKey, ownerId, semaphoreExpireSeconds);
        }
    }

    public boolean extendSemaphore(final Integer semaphoreExpireSeconds) {
        try (Handle handle = rdbi.open()) {
            return ownerId.equals(handle.attach(RedisSemaphoreDAO.class)
                                        .reacquireSemaphore(semaphoreKey, ownerId, semaphoreExpireSeconds));
        }
    }

    public Optional<String> releaseSemaphore() {
        try (Handle handle = rdbi.open()) {
            return Optional.fromNullable(handle.attach(RedisSemaphoreDAO.class)
                                               .releaseSemaphore(semaphoreKey, ownerId));
        }
    }
}
