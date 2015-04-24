package com.lithium.dbi.rdbi.recipes.queue;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.recipes.cache.SerializationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Pipeline;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class RedisSet<ValueType> implements Set<ValueType> {
    public static class ValueWithScore <ValueType> {
        private final ValueType value;
        private final double score;

        public ValueWithScore(ValueType value, double score) {
            this.value = value;
            this.score = score;
        }

        public ValueType getValue() {
            return value;
        }

        public double getScore() {
            return score;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, score);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final ValueWithScore other = (ValueWithScore) obj;
            return Objects.equals(this.value, other.value)
                    && Objects.equals(this.score, other.score);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                              .add("value", value)
                              .add("score", score)
                              .toString();
        }
    }
    private static final Logger log = LoggerFactory.getLogger(RedisSet.class);
    private final SerializationHelper<ValueType> serializationHelper;
    private final String cacheName;
    private final String redisKeyOfSet;
    private final RDBI rdbi;
    private final double defaultScore;

    public RedisSet(SerializationHelper<ValueType> serializationHelper,
                    String cacheName,
                    String redisKeyOfSet,
                    RDBI rdbi,
                    double defaultScore) {
        this.serializationHelper = serializationHelper;
        this.cacheName = cacheName;
        this.redisKeyOfSet = redisKeyOfSet;
        this.rdbi = rdbi;
        this.defaultScore = defaultScore;
    }

    @Override
    public int size() {
        try (final Handle handle = rdbi.open()) {
            final Long size = handle.jedis().zcount(redisKeyOfSet, "-inf", "+inf");
            if (size > Integer.MAX_VALUE) {
                log.info("size of " + cacheName + " exceeds integer max value. .size() just lied to you.");
                return Integer.MAX_VALUE;
            }
            return size.intValue();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    public List<ValueWithScore<ValueType>> popRange(final long startScore, final long endScore) {
        final ImmutableList.Builder<ValueWithScore<ValueType>> builder = ImmutableList.builder();

        try (final Handle handle = rdbi.open()) {
            final List<String> elementsWithScores = handle.attach(SetDAO.class)
                                                          .zPopRangeByRank(redisKeyOfSet, startScore, endScore);
            for (int i = 0; i < elementsWithScores.size(); i += 2) {
                final ValueType value = serializationHelper.decode(elementsWithScores.get(i));
                final double score = Double.valueOf(elementsWithScores.get(i + 1));
                final ValueWithScore<ValueType> valWithScore = new ValueWithScore<>(value, score);
                builder.add(valWithScore);
            }
        }

        return builder.build();
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException("contains not supported by this set");
    }

    @Override
    public Iterator<ValueType> iterator() {
        throw new UnsupportedOperationException("iterator not supported by this set");
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("toArray not supported by this set");
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException("toArray not supported by this set");
    }

    @Override
    public boolean add(ValueType value) {
        return addAll(ImmutableList.of(value));
    }

    @Override
    public boolean addAll(Collection<? extends ValueType> toAdd) {
        return addAll(toAdd, defaultScore);
    }

    public boolean addAll(Collection<? extends ValueType> toAdd, final double score) {
        try (final Handle handle = rdbi.open()) {
            final Pipeline pl = handle.jedis().pipelined();
            for (final ValueType value : toAdd) {
                internalPipelinedAdd(value, pl, score);
            }
            pl.sync();
        }
        return true;
    }

    private void internalPipelinedAdd(final ValueType value, final Pipeline pl, final double score) {
        final String valueAsString = serializationHelper.encode(value);
        pl.zadd(redisKeyOfSet, score, valueAsString);
    }

    @Override
    public boolean remove(Object value) {
        return removeAll(ImmutableList.of(value));
    }

    private void internalPipelinedRemove(ValueType value, Pipeline pl) {
        final String valueAsString = serializationHelper.encode(value);
        pl.zrem(redisKeyOfSet, valueAsString);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("containsAll not supported by this set");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("retainAll not supported by this set");
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeAll(Collection<?> toRemove) {
        try (final Handle handle = rdbi.open()) {
            final Pipeline pl = handle.jedis().pipelined();
            try {
                for (final Object value : toRemove) {
                    final ValueType castValue = (ValueType)value;
                    internalPipelinedRemove(castValue, pl);
                }
            } catch (ClassCastException ex) {
                throw new IllegalArgumentException("Couldn't cast entry in list to set object type!", ex);
            }
            pl.sync();
        }
        return true;
    }

    @Override
    public void clear() {
        try (final Handle handle = rdbi.open()) {
            handle.jedis().del(redisKeyOfSet);
        }
    }
}
