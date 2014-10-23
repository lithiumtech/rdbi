package com.lithium.dbi.rdbi.recipes.cache;

import java.util.concurrent.atomic.AtomicReference;

public class BlockingReference<ValueType> {
    private final AtomicReference<ValueType> ref = new AtomicReference<>();

    public void set(final ValueType val) {
        synchronized (ref) {
            ref.set(val);
            ref.notifyAll();
        }
    }

    public ValueType peek() {
        synchronized (ref) {
            return ref.get();
        }
    }

    public ValueType get() throws InterruptedException {
        synchronized (ref) {
            if(ref.get() != null) {
                return ref.get();
            }
            ref.wait();
            return ref.get();
        }
    }

    public ValueType get(final long waitMillis) throws InterruptedException {
        synchronized(ref) {
            if(ref.get() != null) {
                return ref.get();
            }
            ref.wait(waitMillis);
            return ref.get();
        }
    }
}
