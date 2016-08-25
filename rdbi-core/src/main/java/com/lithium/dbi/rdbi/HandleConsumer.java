package com.lithium.dbi.rdbi;

/**
 * Given a RDBI handle, perform some work.
 */
public interface HandleConsumer {
    void accept(Handle handle);
}
