package com.lithium.dbi.rdbi.recipes.locking;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;

public class Semaphore {
    private final RDBI rdbi;
    private final String ownerId;
    private final String semaphoreKey;

    public Semaphore(RDBI rdbi, String ownerId, String semaphoreKey) {
        this.rdbi = rdbi;
        this.ownerId = ownerId;
        this.semaphoreKey = semaphoreKey;
    }

    public boolean acquireSemaphore(final Integer semaphoreExpireSeconds) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(SemaphoreDAO.class)
                         .acquireSemaphore(ImmutableList.of(semaphoreKey),
                                      ImmutableList.of(ownerId, semaphoreExpireSeconds.toString()));
        }
    }

    public Optional<String> releaseSemaphore() {
        try (Handle handle = rdbi.open()) {
            return Optional.fromNullable(handle.attach(SemaphoreDAO.class)
                                               .releaseSemaphore(ImmutableList.of(semaphoreKey),
                                                            ImmutableList.of(ownerId)));
        }
    }
}