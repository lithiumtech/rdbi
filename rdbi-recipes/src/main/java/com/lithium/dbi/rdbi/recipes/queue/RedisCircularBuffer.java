package com.lithium.dbi.rdbi.recipes.queue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.recipes.cache.SerializationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

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
        boolean success = offer(value);
        if (!success) {
            throw new IllegalStateException("Could not add to this circular buffer");
        } else {
            return success;
        }
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("removing a specific object is not supported by this circular buffer");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        try (final Handle handle = rdbi.open()) {
            final Pipeline pipeline = handle.jedis().pipelined();
            Set<ValueType> currentSet = new HashSet<ValueType>();
            final int size = handle.jedis().llen(key).intValue();
            final Set<Response<String>> responses = Sets.newHashSetWithExpectedSize(size);

            for (int i = 0; i < size; i++) {
                responses.add(pipeline.lindex(key, i));
            }
            pipeline.sync();

            for(final Response<String> res : responses) {
                ValueType value = serializationHelper.decode(res.get());
                currentSet.add(value);
            }

            return currentSet.containsAll(c);
        }
    }

    @Override
    public boolean addAll(Collection<? extends ValueType> toAdd) {
        for (final ValueType value : toAdd) {
            final String valueAsString = serializationHelper.encode(value);
            try (Handle handle = rdbi.open()) {
                String valueStr = serializationHelper.encode(value);
                final int newSize = handle.attach(RedisCircularBufferDAO.class).add(key, valueStr, maxSize);
                boolean success =  newSize >= 0 && newSize <= maxSize;
                if (!success) {
                    return false;
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
        ValueType element = poll();
        if (element == null) {
            throw new NoSuchElementException("Circular buffer is empty, cannot remove first element");
        } else {
            return element;
        }
    }

    @Override
    public ValueType poll() {
        try (Handle handle = rdbi.open()) {
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
        ValueType element = peek();
        if (element == null) {
            throw new NoSuchElementException("Circular buffer is empty, cannot retrieve head");
        } else {
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

    public List<ValueType> peekAll() {
        try (final Handle handle = rdbi.open()) {
            final Pipeline pipeline = handle.jedis().pipelined();
            List<ValueType> currentList = new ArrayList<ValueType>();
            final int size = handle.jedis().llen(key).intValue();
            final List<Response<String>> responses = Lists.newArrayListWithCapacity(size);

            for (int i = 0; i < size; i++) {
                responses.add(pipeline.lindex(key, i));
            }
            pipeline.sync();

            for(final Response<String> res : responses) {
                ValueType value = serializationHelper.decode(res.get());
                currentList.add(value);
            }

            return currentList;
        }
    }

}
