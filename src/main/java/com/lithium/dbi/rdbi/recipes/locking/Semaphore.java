package com.lithium.dbi.rdbi.recipes.locking;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;

public class Semaphore {
    private final RDBI rdbi;
    private final String ownerId;
    private final String lockKey;

    public Semaphore(RDBI rdbi, String ownerId, String lockKey) {
        this.rdbi = rdbi;
        this.ownerId = ownerId;
        this.lockKey = lockKey;
    }

    public boolean acquireLock(final Integer lockExpireSeconds) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(SemaphoreDAO.class)
                         .acquireLock(ImmutableList.of(lockKey),
                                      ImmutableList.of(ownerId, lockExpireSeconds.toString()));
        }
    }

    public Optional<String> releaseLock() {
        try (Handle handle = rdbi.open()) {
            return Optional.fromNullable(handle.attach(SemaphoreDAO.class)
                                               .releaseLock(ImmutableList.of(lockKey),
                                                            ImmutableList.of(ownerId)));
        }
    }
}
