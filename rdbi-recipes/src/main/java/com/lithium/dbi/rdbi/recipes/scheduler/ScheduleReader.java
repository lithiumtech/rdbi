package com.lithium.dbi.rdbi.recipes.scheduler;

import java.util.Optional;
import java.util.function.LongSupplier;
import com.google.common.primitives.Ints;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;

/**
 * Designed to read schedules from a data store, particularly redis. We need to break off reading
 * to a separate class so clients can safely use a read replica. Derived from {@link MultiChannelScheduler}
 */
public class ScheduleReader {
    protected final RDBI rdbi;
    protected final String prefix;
    protected final LongSupplier clock;

    public ScheduleReader(RDBI rdbi, String prefix, LongSupplier clock) {
        this.rdbi = rdbi;
        this.prefix = prefix;
        this.clock = clock;
    }

    public ScheduleReader(RDBI rdbi, String prefix) {
        this(rdbi, prefix, System::currentTimeMillis);
    }

    public long getAllReadyJobCount(String tube) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(MultiChannelSchedulerDAO.class)
                    .getAllReadyJobCount(
                            getMultiChannelCircularBuffer(tube),
                            clock.getAsLong());
        }
    }

    public long getReadyJobCount(String channel, String tube) {
        final String queue = getReadyQueue(channel, tube);
        return rdbi.withHandle(handle -> handle.jedis().zcount(queue, 0, clock.getAsLong()));
    }

    public long getRunningJobCount(String tube) {
        final String queue = getRunningQueue(tube);
        return rdbi.withHandle(handle -> handle.jedis().zcard(queue));
    }

    public Integer getRunningCountForChannel(String channel, String tube) {
        final String key = getRunningCountKey(channel, tube);
        final String count = rdbi.withHandle(h -> h.jedis().get(key));
        return Optional.ofNullable(count)
                .map(Ints::tryParse)
                .orElse(0);
    }

    private String getMultiChannelCircularBuffer(String tube) {
        return prefix + ":multichannel:" + tube + ":circular_buffer";
    }

    private String getTubePrefix(String channel, String tube) {
        return prefix + ":" + channel + ":" + tube;
    }

    private String getReadyQueue(String channel, String tube) {
        return getTubePrefix(channel, tube) + ":ready_queue";
    }

    private String getRunningQueue(String tube) {
        return prefix + ":multichannel:" + tube + ":running_queue";
    }

    private String getRunningCountKey(String channel, String tube) {
        return getTubePrefix(channel, tube) + ":running_count";
    }


}
