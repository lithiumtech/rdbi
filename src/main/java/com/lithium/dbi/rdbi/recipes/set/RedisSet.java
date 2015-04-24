package com.lithium.dbi.rdbi.recipes.set;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import com.lithium.dbi.rdbi.recipes.cache.SerializationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Simple class that handles some of the boilerplate around using Redis sorted sets with actual java pojos.
 *
 * Supports add, remove, contains and size operations.
 * Does NOT support iteration, retain, or toArray operations at this time.
 *
 * @param <ValueType> What you want to store. Don't forget to write a serializer helper for it.
 * @see SerializationHelper
 */
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
    private final String redisSetKey;
    private final RDBI rdbi;
    private final double defaultScore;

    public RedisSet(SerializationHelper<ValueType> serializationHelper,
                    String cacheName,
                    String redisSetKey,
                    RDBI rdbi,
                    double defaultScore) {
        this.serializationHelper = serializationHelper;
        this.cacheName = cacheName;
        this.redisSetKey = redisSetKey;
        this.rdbi = rdbi;
        this.defaultScore = defaultScore;
    }

    @Override
    public int size() {
        try (final Handle handle = rdbi.open()) {
            final Long size = handle.jedis().zcount(redisSetKey, "-inf", "+inf");
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

    /**
     * Removes and returns elements by redis rank - that is their position in the sorted set.
     * The lowest-scored element is at index 0.
     *
     * Asking for more elements than exist in the set will just get you the entire set.
     *
     * (This does a ZRANGE underneath the covers to select the elements, so all the same rules apply.)
     *
     * This uses a small Lua snippet atomically get and remove the elements from the set.
     * Beware that the entirety of the elements to be returned are stored for a short time in a LUA local variable,
     * so if you ask for a huge huge number of big elements, you could cause problems for yourself on your Redis server.
     * @param lowerRank - start index. lowest-scored element is at 0.
     * @param upperRank - end index. -1 to grab it all.
     * @return Ordered list of the elements popped along with their scores.
     */
    public List<ValueWithScore<ValueType>> popRange(final long lowerRank, final long upperRank) {
        final ImmutableList.Builder<ValueWithScore<ValueType>> builder = ImmutableList.builder();

        try (final Handle handle = rdbi.open()) {
            final List<String> elementsWithScores = handle.attach(SetDAO.class)
                                                          .zPopRangeByRank(redisSetKey, lowerRank, upperRank);
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
        return containsAll(ImmutableList.of(o));
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsAll(Collection<?> toCheck) {
        try (final Handle handle = rdbi.open()) {
            final Pipeline pipeline = handle.jedis().pipelined();
            final List<Response<Long>> responses = Lists.newArrayListWithCapacity(toCheck.size());

            for(final Object obj : toCheck) {
                final ValueType castValue = (ValueType)obj;
                final String valueAsString = serializationHelper.encode(castValue);
                responses.add(pipeline.zrank(redisSetKey, valueAsString));
            }

            pipeline.sync();

            for(final Response<Long> resp : responses) {
                if(resp.get() == null) {
                    return false;
                }
            }
        }

        return true;
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
        pl.zadd(redisSetKey, score, valueAsString);
    }

    @Override
    public boolean remove(Object value) {
        return removeAll(ImmutableList.of(value));
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeAll(Collection<?> toRemove) {
        try (final Handle handle = rdbi.open()) {
            final Pipeline pl = handle.jedis().pipelined();
            for (final Object value : toRemove) {
                final ValueType castValue = (ValueType)value;
                internalPipelinedRemove(castValue, pl);
            }
            pl.sync();
        }
        return true;
    }

    private void internalPipelinedRemove(ValueType value, Pipeline pl) {
        final String valueAsString = serializationHelper.encode(value);
        pl.zrem(redisSetKey, valueAsString);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("retainAll not supported by this set");
    }

    @Override
    public void clear() {
        try (final Handle handle = rdbi.open()) {
            handle.jedis().del(redisSetKey);
        }
    }
}
