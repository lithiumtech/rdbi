package com.lithium.dbi.rdbi.recipes.queue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.recipes.cache.SerializationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * A circular buffer in redis. Head of the queue is the element at index 0 in redis.
 */
public class RedisCircularBuffer<ValueType> implements Queue<ValueType> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCircularBuffer.class);

    private final String key;
    private final RDBI rdbi;
    private final SerializationHelper<ValueType> serializationHelper;
    private final int maxSize;

    public RedisCircularBuffer(final RDBI rdbi,
                               @Nonnull final String key,
                               final int maxSize,
                               SerializationHelper<ValueType> serializationHelper) {
        this.rdbi = rdbi;
        this.key = key;
        this.maxSize = maxSize;
        this.serializationHelper = serializationHelper;
        Preconditions.checkNotNull(key, "A null value was supplied for 'key'.");
    }

    @Override
    public int size() {
        try (final Handle handle = rdbi.open()) {
            final Long size = handle.jedis().llen(key);
            if (size > Integer.MAX_VALUE) {
                LOGGER.info("size of " + key + " exceeds integer max value. .size() just lied to you.");
                return Integer.MAX_VALUE;
            }
            return size.intValue();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return containsAll(ImmutableList.of(o));
    }

    @Override
    public Iterator<ValueType> iterator() {
        throw new UnsupportedOperationException("iterator not supported by this circular buffer");
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("toArray not supported by this circular buffer");
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException("toArray not supported by this circular buffer");
    }

    @Override
    public boolean add(ValueType value) {
        return addAll(ImmutableList.of(value));
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("removing a specific object is not supported by this circular buffer");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        try (final Handle handle = rdbi.open()) {
            for (final Object obj : c) {
                final ValueType valueToFind = (ValueType) obj;
                final String valueToFindStr = serializationHelper.encode(valueToFind);
                boolean found = false;
                for (int i = 0; i < size(); i++) {
                    String valueAtIndex = handle.jedis().lindex(key, i);
                    if (valueToFindStr.equals(valueAtIndex)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends ValueType> toAdd) {
        try (final Handle handle = rdbi.open()) {
            for (final ValueType value : toAdd) {
                final String valueAsString = serializationHelper.encode(value);
                handle.jedis().rpush(key, valueAsString);
                if (size() >= maxSize) {
                    handle.jedis().lpop(key);
                }
            }
        }
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("removeAll not supported by this circular buffer");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("retainAll not supported by this circular buffer");
    }

    @Override
    public void clear() {
        try (final Handle handle = rdbi.open()) {
            handle.jedis().del(key);
        }
    }

    @Override
    public boolean offer(ValueType value) {
        return addAll(ImmutableList.of(value));
    }

    @Override
    public ValueType remove() {
        try (final Handle handle = rdbi.open()) {
            final String removedStr = handle.jedis().lpop(key);
            if (removedStr == null) {
                throw new NoSuchElementException("Circular buffer is empty, cannot remove");
            }
            final ValueType removedValue = serializationHelper.decode(removedStr);
            return removedValue;
        }
    }

    @Override
    public ValueType poll() {
        try (final Handle handle = rdbi.open()) {
            final String removedStr = handle.jedis().lpop(key);
            if (removedStr == null) {
                return null;
            }
            final ValueType removedValue = serializationHelper.decode(removedStr);
            return removedValue;
        }
    }

    @Override
    public ValueType element() {
        try (final Handle handle = rdbi.open()) {
            final String elementStr = handle.jedis().lindex(key, 0);
            if (elementStr == null) {
                throw new NoSuchElementException("Circular buffer is empty, cannot retrieve head");
            }
            final ValueType element = serializationHelper.decode(elementStr);
            return element;
        }
    }

    @Override
    public ValueType peek() {
        try (final Handle handle = rdbi.open()) {
            final String removedStr = handle.jedis().lindex(key, 0);
            if (removedStr == null) {
                return null;
            }
            final ValueType removedValue = serializationHelper.decode(removedStr);
            return removedValue;
        }
    }
}
